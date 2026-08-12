#!/usr/bin/env bash
#
# monitor-agent 安装 / 升级 / 开机自启 / 卸载 脚本（需 root 权限）
#
# 用法：
#   sudo bash install.sh                 # 仅安装/升级程序文件（默认）
#   sudo bash install.sh systemd         # 安装文件 + 注册 systemd 开机自启并立即启动
#   sudo bash install.sh systemd-remove  # 停止并移除 systemd 服务（保留程序文件）
#   sudo bash install.sh uninstall       # 完全卸载（停止服务 + 删除已安装文件，备份保留）
#
# 行为：
#   1. 源目录自动定位为本脚本所在目录（克隆仓库后直接运行即可，不依赖固定路径）；
#   2. 备份目标目录现有文件到 <目标目录>.bak-<时间戳>；
#   3. 覆盖安装核心代码、README、DOCUMENTATION、配置示例、requirements 与 systemd 模板；
#   4. 刷新 <目标目录>.zip 归档（若本机有 zip 命令，旧包同样留备份）；
#   5. systemd 模式：注册 monitor-agent.service 开机自启并立即启动，
#      同时生成 /etc/monitor-agent/env 密钥模板（root:root 0600）。
#
# 安装位置默认 /opt/monitor-agent，可用环境变量覆盖：
#   sudo MONITOR_AGENT_DIR=/usr/local/monitor-agent bash install.sh systemd
set -euo pipefail

# 源目录 = 脚本自身所在目录（处理符号链接场景）
SRC=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
DST=${MONITOR_AGENT_DIR:-/opt/monitor-agent}
APP_ZIP=${DST}.zip
SERVICE_NAME=monitor-agent.service
UNIT_PATH=/etc/systemd/system/${SERVICE_NAME}
ETC_DIR=/etc/monitor-agent
ENV_FILE=${ETC_DIR}/env
CONFIG_EXAMPLE=${ETC_DIR}/config.example.json
TS=$(date +%Y%m%d-%H%M%S)
BAK=${DST}.bak-${TS}

MODE="${1:-files}"

require_root() {
    if [[ $EUID -ne 0 ]]; then
        echo "错误：需要 root 权限。请使用: sudo bash $0 ${MODE}" >&2
        exit 1
    fi
}

install_files() {
    if [[ ! -d "$SRC" ]]; then
        echo "错误：未找到源目录 $SRC" >&2
        exit 1
    fi

    mkdir -p "$DST" "$BAK"
    cp -p "$DST"/*.py "$BAK"/ 2>/dev/null || true
    echo "[1/5] 已备份现有代码到 $BAK"

    cp -p "$SRC"/main.py "$SRC"/config.py "$SRC"/collectors.py \
          "$SRC"/alerting.py "$SRC"/log_monitor.py "$DST"/
    cp -p "$SRC"/README.md "$SRC"/DOCUMENTATION.md "$DST"/
    cp -p "$SRC"/requirements.txt "$SRC"/config.example.json "$DST"/
    mkdir -p "$DST"/deploy
    sed "s|__MONITOR_DIR__|${DST//&/\\&}|g" \
        "$SRC"/deploy/monitor-agent.service.example \
        > "$DST"/deploy/monitor-agent.service.example
    chmod 755 "$DST"/*.py
    echo "[2/5] 已完成覆盖安装到 $DST"

    if command -v python3 >/dev/null 2>&1; then
        python3 -m py_compile "$DST"/*.py
        echo "[3/5] 语法检查通过"
    else
        echo "[3/5] 跳过语法检查（未找到 python3）"
    fi

    if command -v zip >/dev/null 2>&1; then
        cp -p "$APP_ZIP" "$APP_ZIP.bak-${TS}" 2>/dev/null || true
        tmp=$(mktemp)
        rm -f "$tmp"
        (cd "$DST" && zip -q -r "$tmp" . -x '__pycache__/*' -x '*.pyc')
        cp -p "$tmp" "$APP_ZIP"
        rm -f "$tmp"
        echo "[4/5] 已刷新 $APP_ZIP（旧包备份为 $APP_ZIP.bak-${TS}）"
    else
        echo "[4/5] 跳过 zip 归档刷新（未找到 zip 命令）"
    fi

    echo "[5/5] 程序文件安装完成"
    echo
    echo "快速验证："
    echo "  python3 $DST/main.py --selftest"
    echo "  python3 $DST/main.py"
    echo
    echo "原文件备份在："
    echo "  $BAK"
}

write_env_template() {
    mkdir -p "$ETC_DIR"
    if [[ -f "$CONFIG_EXAMPLE" ]]; then
        echo "  [跳过] 配置示例已存在: $CONFIG_EXAMPLE"
    else
        cp -p "$SRC"/config.example.json "$CONFIG_EXAMPLE"
        echo "  [OK] 已写入配置示例: $CONFIG_EXAMPLE"
    fi
    if [[ -f "$ENV_FILE" ]]; then
        echo "  [跳过] 密钥模板已存在（如需修改请编辑）: $ENV_FILE"
        chmod 600 "$ENV_FILE"
        return
    fi
    cat > "$ENV_FILE" <<'EOF'
# monitor-agent 环境配置（建议保持 root:root 0600）
# 钉钉机器人 Webhook：留空时告警仅本地留痕，不推送
DINGTALK_WEBHOOK=
DINGTALK_SECRET=

# 按主机启用服务/日志监控（三者任选其一，详见 DOCUMENTATION.md）
# MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
# MONITOR_SERVICES=[{"name":"nginx","process_names":["nginx"],"port":80}]
# MONITOR_LOG_JOBS=[{"name":"nginx_error","path":"/var/log/nginx/error.log","patterns":[["connect\\(\\) failed","NGINX_UPSTREAM_FAIL","Nginx 后端网关异常"]]}]

# 采集与告警参数（可选，默认值见 DOCUMENTATION.md）
# MONITOR_INTERVAL=60
# LOG_SCAN_INTERVAL=10
# ALERT_COOLDOWN=300
# 阈值覆盖示例（默认见 DOCUMENTATION.md）
# CPU_PERCENT_WARNING=85
# CPU_PERCENT_CRITICAL=97
# ALERT_STATE_FILE=/var/lib/monitor-agent/alert-state.json
EOF
    chmod 600 "$ENV_FILE"
    echo "  [OK] 已生成密钥模板: $ENV_FILE（请填入 DINGTALK_WEBHOOK 后重启服务）"
}

install_systemd() {
    require_root
    install_files
    echo
    echo "== 注册 systemd 开机自启 =="
    sed "s|__MONITOR_DIR__|${DST//&/\\&}|g" \
        "$SRC"/deploy/monitor-agent.service.example > "$UNIT_PATH"
    chmod 644 "$UNIT_PATH"
    echo "  [OK] 已安装服务单元: $UNIT_PATH"
    write_env_template
    systemctl daemon-reload
    systemctl enable --now "$SERVICE_NAME"
    systemctl --no-pager --lines=20 status "$SERVICE_NAME" || true
    echo
    echo "开机自启已启用。常用命令："
    echo "  systemctl status $SERVICE_NAME"
    echo "  journalctl -u $SERVICE_NAME -f"
    echo "  systemctl restart $SERVICE_NAME"
}

remove_systemd() {
    require_root
    echo "== 移除 systemd 服务（保留程序文件） =="
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
    rm -f "$UNIT_PATH"
    systemctl daemon-reload
    echo "  [OK] 已停止并移除服务 $SERVICE_NAME"
    echo "  [提示] 如需恢复，请再次执行: sudo bash $0 systemd"
}

uninstall_all() {
    require_root
    echo "== 完全卸载 =="
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
    rm -f "$UNIT_PATH"
    systemctl daemon-reload
    rm -f "$APP_ZIP"
    rm -rf "$DST"
    echo "  [OK] 已删除服务单元、$DST 与 $APP_ZIP"
    echo "  [提示] 程序文件备份仍保留在:"
    ls -d "$DST".bak-* 2>/dev/null || echo "    （无备份）"
}

case "$MODE" in
    files|"")
        require_root
        install_files
        ;;
    systemd)
        install_systemd
        ;;
    systemd-remove)
        remove_systemd
        ;;
    uninstall)
        uninstall_all
        ;;
    *)
        echo "未知参数: $MODE" >&2
        echo "用法: sudo bash $0 [files|systemd|systemd-remove|uninstall]" >&2
        exit 2
        ;;
esac
