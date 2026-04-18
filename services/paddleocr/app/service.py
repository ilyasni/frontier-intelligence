import base64
import io
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import structlog
from PIL import Image

logger = structlog.get_logger(__name__)

try:
    from paddleocr import PaddleOCR
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "PaddleOCR/paddle import failed (проверь pip-зависимости и образ paddlepaddle/paddle)."
    ) from exc


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        logger.warning("Invalid int env value, using default", env=name, default=default)
        return default


@dataclass
class OCRConfig:
    """Настройки PaddleOCR, подгружаемые из env."""

    languages: Tuple[str, ...] = ("cyrillic",)
    rec_algorithm: str = os.getenv("LOCAL_OCR_REC_ALGO", "SVTR_LCNet")
    det_model_dir: Optional[str] = os.getenv("LOCAL_OCR_DET_MODEL_DIR")
    rec_model_dir: Optional[str] = os.getenv("LOCAL_OCR_REC_MODEL_DIR")
    cls_model_dir: Optional[str] = os.getenv("LOCAL_OCR_CLS_MODEL_DIR")
    use_angle_cls: bool = _env_flag("LOCAL_OCR_USE_ANGLE", True)
    use_gpu: bool = _env_flag("LOCAL_OCR_USE_GPU", False)
    lang_priority: Tuple[str, ...] = field(
        default_factory=lambda: tuple(
            os.getenv("LOCAL_OCR_LANG_PRIORITY", "cyrillic").replace(" ", "").split(",")
        )
    )
    det_limit_side_len: int = _env_int("LOCAL_OCR_DET_SIDE_LEN", 960)
    rec_image_shape: str = os.getenv("LOCAL_OCR_REC_IMAGE_SHAPE", "3, 48, 320")

    def __post_init__(self) -> None:
        normalized = []
        for lang in self.languages:
            lang = lang.strip()
            if lang:
                normalized.append(lang)
        self.languages = tuple(normalized or ("cyrillic",))
        # Пустая строка из docker-compose (${VAR:-}) — как «не задано»
        for attr in ("det_model_dir", "rec_model_dir", "cls_model_dir"):
            if not getattr(self, attr):
                setattr(self, attr, None)


class PaddleOCRService:
    """Обёртка над PaddleOCR с lazy-инициализацией и стандартным форматом ответа."""

    def __init__(self, config: Optional[OCRConfig] = None) -> None:
        self.config = config or OCRConfig()
        self._lock = threading.Lock()
        self._ocr: Optional[PaddleOCR] = None
        self._models_ready: bool = False
        self._models_error: Optional[str] = None
        logger.info(
            "PaddleOCRService initialized",
            languages=self.config.languages,
            use_gpu=self.config.use_gpu,
            rec_algorithm=self.config.rec_algorithm,
        )

    def is_models_ready(self) -> bool:
        with self._lock:
            return self._models_ready

    def get_models_error(self) -> Optional[str]:
        with self._lock:
            return self._models_error

    def _ensure_loaded(self) -> PaddleOCR:
        if self._ocr is not None:
            return self._ocr
        with self._lock:
            if self._ocr is not None:
                return self._ocr
            start = time.time()
            logger.info("Loading PaddleOCR models", languages=self.config.languages)
            try:
                self._ocr = PaddleOCR(
                    use_angle_cls=self.config.use_angle_cls,
                    lang="+".join(self.config.languages),
                    rec_algorithm=self.config.rec_algorithm,
                    det_model_dir=self.config.det_model_dir,
                    rec_model_dir=self.config.rec_model_dir,
                    cls_model_dir=self.config.cls_model_dir,
                    use_gpu=self.config.use_gpu,
                    det_limit_side_len=self.config.det_limit_side_len,
                    rec_image_shape=self.config.rec_image_shape,
                    show_log=False,
                )
                self._models_ready = True
                self._models_error = None
                duration = time.time() - start
                logger.info("PaddleOCR models loaded", duration_ms=int(duration * 1000))
            except Exception as exc:
                self._models_ready = False
                self._models_error = str(exc)[:500]
                logger.exception("PaddleOCR model load failed", error=self._models_error)
                raise
        return self._ocr

    def run_ocr(
        self,
        image_bytes: bytes,
        languages: Optional[List[str]] = None,
        return_image: bool = False,
    ) -> Dict[str, Any]:
        """Выполнить OCR над изображением."""
        ocr = self._ensure_loaded()
        selected_langs = self._validate_languages(languages)

        pil_image = Image.open(io.BytesIO(image_bytes))
        pil_image = pil_image.convert("RGB")
        image_np = np.asarray(pil_image)

        start = time.time()
        result = ocr.ocr(image_np, det=True, rec=True, cls=self.config.use_angle_cls)
        duration = time.time() - start

        lines = self._normalize_predictions(result)

        response: Dict[str, Any] = {
            "language_priority": selected_langs,
            "lines": lines,
            "aggregates": self._aggregate(lines, duration),
            "duration_ms": int(duration * 1000),
        }

        if return_image:
            buffered = io.BytesIO()
            pil_image.save(buffered, format="JPEG")
            response["image_base64"] = base64.b64encode(buffered.getvalue()).decode()

        logger.debug(
            "OCR processed image",
            duration_ms=response["duration_ms"],
            line_count=len(lines),
            avg_confidence=response["aggregates"]["confidence_mean"],
        )
        return response

    def _validate_languages(self, languages: Optional[List[str]]) -> Tuple[str, ...]:
        if not languages:
            return self.config.lang_priority
        valid = []
        for lang in languages:
            if lang in self.config.languages:
                valid.append(lang)
            else:
                logger.warning("Requested OCR language is not enabled", language=lang)
        return tuple(valid or self.config.lang_priority)

    @staticmethod
    def _normalize_predictions(result: List[Any]) -> List[Dict[str, Any]]:
        lines: List[Dict[str, Any]] = []
        if not result:
            return lines
        for block in result:
            if block is None:
                continue
            for item in block:
                if not item or len(item) != 2:
                    continue
                bbox_points, text_info = item
                text = ""
                confidence = 0.0
                if isinstance(text_info, (tuple, list)) and len(text_info) >= 2:
                    text = str(text_info[0])
                    confidence = float(text_info[1])
                bbox = [
                    {"x": float(point[0]), "y": float(point[1])}
                    for point in (bbox_points or [])
                ]
                lines.append(
                    {
                        "text": text,
                        "confidence": confidence,
                        "bbox": bbox,
                    }
                )
        return lines

    @staticmethod
    def _aggregate(lines: List[Dict[str, Any]], duration: float) -> Dict[str, Any]:
        if not lines:
            return {
                "line_count": 0,
                "confidence_mean": 0.0,
                "confidence_min": 0.0,
                "confidence_max": 0.0,
                "throughput_img_per_s": 0.0 if duration == 0 else 1.0 / duration,
            }
        confidences = [line["confidence"] for line in lines]
        return {
            "line_count": len(lines),
            "confidence_mean": sum(confidences) / len(confidences),
            "confidence_min": min(confidences),
            "confidence_max": max(confidences),
            "throughput_img_per_s": 0.0 if duration == 0 else 1.0 / duration,
        }


_DEFAULT_SERVICE: Optional[PaddleOCRService] = None
_SERVICE_LOCK = threading.Lock()


def get_default_service() -> PaddleOCRService:
    global _DEFAULT_SERVICE
    if _DEFAULT_SERVICE is None:
        with _SERVICE_LOCK:
            if _DEFAULT_SERVICE is None:
                _DEFAULT_SERVICE = PaddleOCRService()
    return _DEFAULT_SERVICE
