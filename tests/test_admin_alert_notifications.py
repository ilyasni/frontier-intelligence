from admin.backend.services.telegram_alerts import format_alertmanager_message


def test_format_alertmanager_message_includes_core_context() -> None:
    message = format_alertmanager_message(
        {
            "status": "firing",
            "commonLabels": {
                "alertname": "FrontierCoreServiceDown",
                "severity": "critical",
                "service": "worker",
            },
            "commonAnnotations": {
                "summary": "Core Frontier service is down",
                "description": "worker is unreachable",
            },
            "alerts": [
                {
                    "labels": {
                        "job": "worker",
                        "instance": "worker:9090",
                    }
                }
            ],
        }
    )

    assert "Frontier FIRING: FrontierCoreServiceDown" in message
    assert "severity: CRITICAL" in message
    assert "service: worker" in message
    assert "summary: Core Frontier service is down" in message
    assert "job=worker instance=worker:9090" in message
