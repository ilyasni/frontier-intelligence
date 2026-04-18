#!/bin/sh
set -eu

cat > /etc/xray/config.json <<EOF
{
  "log": {
    "loglevel": "${XRAY_LOG_LEVEL:-warning}"
  },
  "inbounds": [
    {
      "tag": "socks-in",
      "listen": "0.0.0.0",
      "port": ${XRAY_SOCKS_PORT:-10808},
      "protocol": "socks",
      "settings": {
        "udp": true
      },
      "sniffing": {
        "enabled": true,
        "destOverride": ["http", "tls", "quic"]
      }
    }
  ],
  "outbounds": [
    {
      "tag": "proxy",
      "protocol": "vless",
      "settings": {
        "vnext": [
          {
            "address": "${XRAY_VLESS_ADDRESS}",
            "port": ${XRAY_VLESS_PORT:-443},
            "users": [
              {
                "id": "${XRAY_VLESS_ID}",
                "encryption": "none",
                "flow": "${XRAY_VLESS_FLOW:-xtls-rprx-vision}"
              }
            ]
          }
        ]
      },
      "streamSettings": {
        "network": "tcp",
        "security": "reality",
        "realitySettings": {
          "fingerprint": "${XRAY_REALITY_FP:-chrome}",
          "serverName": "${XRAY_REALITY_SNI}",
          "publicKey": "${XRAY_REALITY_PBK}",
          "shortId": "${XRAY_REALITY_SID}",
          "spiderX": "/"
        }
      }
    },
    {
      "tag": "direct",
      "protocol": "freedom",
      "settings": {}
    }
  ]
}
EOF

exec /usr/local/bin/xray -config /etc/xray/config.json
