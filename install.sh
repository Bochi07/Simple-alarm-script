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
#   3. 覆盖安装核心代码、README、配置示例、requirements 与 systemd 模板；
#   4. 刷新 <目标目录>.zip 归档（若本机有 zip 命令，旧包同样留备份）；
#   5. systemd 模式：注册 monitor-agent.service 开机自启并立即启动，
#      同时生成 /etc/monitor-agent/env 密钥模板（root:root 0600）。
#   6. 自动探测 python3 路径并写入服务单元；自动检测 systemd 版本，
#      <235（如 CentOS 7）改用兼容模板（legacy）；
#   7. 依赖检查：探测 Python 依赖 psutil 是否可用；缺失时打印告警与修复命令，
#      但不中止安装（systemd 模式会再给出醒目提示），避免装出反复重启的服务；
#   8. 备份为全量备份（排除 __pycache__ 与 *.pyc），只保留最近 N 个 .bak-* 目录
#      （N 默认 5，可用 MONITOR_AGENT_BAK_KEEP 调整），更早的备份删除前会先提示；
#      全新安装（目标目录不存在或为空）不创建空备份目录；
#   9. 安全护栏：拒绝把安装目录设为主目录、根目录、源码仓库自身/其上级目录、
#      系统关键目录（见 SYSTEM_DIR_BLACKLIST）以及深度不足的短路径，
#      防止卸载时误删重要数据；校验与删除统一使用 realpath 规范化后的路径。
#  10. 卸载护栏：删除前打印将要删除的绝对路径；非交互终端或 MONITOR_AGENT_YES=1
#      时跳过确认，交互模式下必须输入 yes 才继续（默认不删除）。
#
# 安装位置默认 /opt/monitor-agent，可用环境变量覆盖：
#   sudo MONITOR_AGENT_DIR=/usr/local/monitor-agent bash install.sh systemd
#   sudo MONITOR_AGENT_YES=1 bash install.sh uninstall     # 非交互确认删除
set -euo pipefail

# 系统关键目录黑名单：卸载会对目标目录执行 rm -rf，这些路径一律拒绝
SYSTEM_DIR_BLACKLIST=(
    /bin /boot /dev /etc /home /lib /lib32 /lib64 /media /mnt
    /opt /proc /root /run /sbin /srv /sys /tmp /usr /var
)
# 备份保留数量：仅保留最近 N 个 <目标目录>.bak-* 目录（默认 5）
BAK_KEEP=${MONITOR_AGENT_BAK_KEEP:-5}
# psutil 探测结果：-1=未探测，0=缺失，1=可用
PSUTIL_OK=-1

canon_path() {
    # 规范化路径：优先 realpath -m（不要求路径已存在，可去掉尾斜杠/解析 . 与 ..）；
    # realpath 不可用时至少去掉尾部斜杠，避免 "/usr/" 这类写法绕过校验
    local p=$1 out
    if out=$(realpath -m -- "$p" 2>/dev/null) && [[ -n "$out" ]]; then
        printf '%s' "$out"
        return 0
    fi
    while [[ "$p" != "/" && "$p" == */ ]]; do
        p=${p%/}
    done
    printf '%s' "${p:-/}"
}

# 源目录 = 脚本自身所在目录（处理符号链接场景）
SRC=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
# 安装目录统一使用规范化后的绝对路径：校验、备份命名与卸载删除都基于该值
DST=$(canon_path "${MONITOR_AGENT_DIR:-/opt/monitor-agent}")
APP_ZIP=${DST}.zip
SERVICE_NAME=monitor-agent.service
UNIT_PATH=/etc/systemd/system/${SERVICE_NAME}
ETC_DIR=/etc/monitor-agent
ENV_FILE=${ETC_DIR}/env
CONFIG_EXAMPLE=${ETC_DIR}/config.example.json
PYTHON_BIN=${MONITOR_AGENT_PYTHON:-$(command -v python3 || true)}
TS=$(date +%Y%m%d-%H%M%S)-$$
BAK=${DST}.bak-${TS}

MODE="${1:-files}"

require_root() {
    if [[ $EUID -ne 0 ]]; then
        echo "错误：需要 root 权限。请使用: sudo bash $0 ${MODE}" >&2
        exit 1
    fi
}

guard_target() {
    # 防止 MONITOR_AGENT_DIR 指向危险路径导致 uninstall 误删重要目录
    local canonical home_canon src_canon d part depth
    canonical=$(canon_path "$DST")
    DST=$canonical   # 后续安装/卸载一律使用规范化后的路径
    if [[ -z "$canonical" || "$canonical" == "/" ]]; then
        echo "错误：MONITOR_AGENT_DIR 无效（不能为空或根目录）: $DST" >&2
        exit 1
    fi
    # 系统关键目录黑名单（精确匹配：/opt 拒绝，/opt/monitor-agent 允许）
    for d in "${SYSTEM_DIR_BLACKLIST[@]}"; do
        if [[ "$canonical" == "$d" ]]; then
            echo "错误：MONITOR_AGENT_DIR 不能是系统关键目录: $canonical" >&2
            echo "       请使用其下的子目录，例如 /opt/monitor-agent" >&2
            exit 1
        fi
    done
    # 深度护栏：根记 1 级（/data 记 2 级，/opt/monitor-agent 记 3 级），要求深度 >= 3，
    # 即安装目录必须形如 /<父目录>/<子目录>。这样即使 MONITOR_AGENT_DIR=/data 这类
    # 独立挂载点根目录（不在系统目录黑名单里）也会被拒绝——uninstall 的 rm -rf 不能
    # 作用在挂载点根目录上，否则会连带删除该挂载点的全部数据。
    depth=1
    local -a parts=()
    IFS='/' read -r -a parts <<< "$canonical"
    for part in "${parts[@]}"; do
        [[ -n "$part" ]] && depth=$((depth + 1))
    done
    if (( depth < 3 )); then
        echo "错误：MONITOR_AGENT_DIR 路径过浅（需要 /<父目录>/<子目录>，如 /opt/monitor-agent）: $canonical" >&2
        exit 1
    fi
    home_canon=$(canon_path "$HOME")
    if [[ "$canonical" == "$home_canon" || "$canonical" == "$home_canon"/* ]]; then
        echo "错误：不允许把 MONITOR_AGENT_DIR 设为主目录或其子目录: $canonical" >&2
        exit 1
    fi
    src_canon=$(canon_path "$SRC")
    if [[ "$canonical" == "$src_canon" || "$canonical" == "$src_canon"/* ]]; then
        echo "错误：MONITOR_AGENT_DIR 不能是源码仓库自身或其子目录: $canonical" >&2
        echo "       （卸载时会删除该目录，请换一个安装位置，如 /opt/monitor-agent）" >&2
        exit 1
    fi
    if [[ "$src_canon" == "$canonical"/* ]]; then
        echo "错误：MONITOR_AGENT_DIR（$canonical）是源码仓库的上级目录" >&2
        echo "       （卸载时会连带删除源码仓库，请换一个安装位置，如 /opt/monitor-agent）" >&2
        exit 1
    fi
}

sed_repl_escape() {
    # 转义 sed 替换段中的特殊字符：反斜杠、& 以及分隔符 |，
    # 避免安装路径含 | 或 & 时破坏 sed 表达式（配合 s|...|...|g 使用）
    local s=$1
    s=${s//\\/\\\\}
    s=${s//&/\\&}
    s=${s//|/\\|}
    printf '%s' "$s"
}

check_python() {
    # 返回 0 表示可用；缺失或版本过低时输出错误并返回 1
    if [[ -z "$PYTHON_BIN" ]]; then
        echo "错误：未找到 python3，请先安装 Python >= 3.8（或设置 MONITOR_AGENT_PYTHON）" >&2
        return 1
    fi
    if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 8) else 1)' 2>/dev/null; then
        echo "错误：需要 Python >= 3.8，当前 $PYTHON_BIN 版本过低" >&2
        return 1
    fi
    local ver
    ver=$("$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    echo "  [OK] Python: $PYTHON_BIN ($ver)"
    return 0
}

check_psutil() {
    # 探测 Python 依赖 psutil：缺失时只告警并给出修复命令，不中止安装
    # （服务会因 ModuleNotFoundError: psutil 反复重启，因此在结尾还会汇总提示）
    if [[ -z "$PYTHON_BIN" ]]; then
        return 0   # 无可用 Python 时由 check_python 负责提示
    fi
    if "$PYTHON_BIN" -c 'import psutil' 2>/dev/null; then
        PSUTIL_OK=1
        echo "  [OK] 依赖 psutil 已安装"
        return 0
    fi
    PSUTIL_OK=0
    {
        echo "  [警告] 未检测到 Python 依赖 psutil（$PYTHON_BIN -c 'import psutil' 失败）"
        echo "         服务启动后会因 ModuleNotFoundError: psutil 反复重启，请先安装依赖："
        echo "           \"$PYTHON_BIN\" -m pip install -r $DST/requirements.txt"
    } >&2
    return 0
}

cleanup_pycache() {
    # py_compile 会在安装目录生成 __pycache__/*.pyc，安装完成后清理，避免残留
    local dir=$1
    [[ -d "$dir" ]] || return 0
    find "$dir" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
    find "$dir" -type f -name '*.pyc' -delete 2>/dev/null || true
    return 0
}

backup_install_dir() {
    # 全量备份目标目录（排除 __pycache__ 与 *.pyc），以便按 README 的承诺回滚；
    # 目标目录不存在或为空（全新安装）时不创建空备份目录
    local src=$1 bak=$2
    if [[ ! -d "$src" ]]; then
        return 0
    fi
    if [[ -z "$(ls -A "$src" 2>/dev/null || true)" ]]; then
        return 0
    fi
    mkdir -p "$bak"
    if command -v rsync >/dev/null 2>&1; then
        rsync -a --exclude='__pycache__' --exclude='*.pyc' "$src"/ "$bak"/
    else
        cp -a "$src"/. "$bak"/
        find "$bak" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
        find "$bak" -type f -name '*.pyc' -delete 2>/dev/null || true
    fi
    return 0
}

prune_backups() {
    # 只保留最近 N 个 <目标目录>.bak-* 备份，更早的删除前先打印一行说明
    local keep=$BAK_KEEP f idx=0
    [[ "$keep" =~ ^[0-9]+$ ]] || keep=5
    (( keep >= 1 )) || keep=1
    local -a baks=() sorted=()
    shopt -s nullglob
    baks=( "$DST".bak-* )
    shopt -u nullglob
    ((${#baks[@]} > keep)) || return 0
    while IFS= read -r f; do
        sorted+=( "$f" )
    done < <(printf '%s\n' "${baks[@]}" | LC_ALL=C sort -r)
    for f in "${sorted[@]}"; do
        idx=$((idx + 1))
        (( idx <= keep )) && continue
        echo "  [清理] 备份目录超过 $keep 个，删除更早的备份: $f"
        rm -rf "${f:?refusing to remove empty backup path}"
    done
    return 0
}

systemd_available() {
    [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1
}

systemd_version() {
    systemctl --version 2>/dev/null | sed -n '1s/.* \([0-9][0-9]*\).*/\1/p'
}

install_files() {
    guard_target
    if [[ ! -d "$SRC" ]]; then
        echo "错误：未找到源目录 $SRC" >&2
        exit 1
    fi

    mkdir -p "$DST"
    backup_install_dir "$DST" "$BAK"
    if [[ -d "$BAK" ]]; then
        echo "[1/5] 已备份现有文件到 $BAK"
    else
        echo "[1/5] 全新安装（目标目录不存在或为空），未创建备份目录"
    fi

    cp -p "$SRC"/main.py "$SRC"/config.py "$SRC"/collectors.py \
          "$SRC"/alerting.py "$SRC"/log_monitor.py "$DST"/
    cp -p "$SRC"/README.md "$DST"/
    cp -p "$SRC"/requirements.txt "$SRC"/config.example.json "$DST"/
    mkdir -p "$DST"/deploy
    local dst_esc
    dst_esc=$(sed_repl_escape "$DST")
    sed "s|__MONITOR_DIR__|${dst_esc}|g" \
        "$SRC"/deploy/monitor-agent.service.example \
        > "$DST"/deploy/monitor-agent.service.example
    sed "s|__MONITOR_DIR__|${dst_esc}|g" \
        "$SRC"/deploy/monitor-agent.service.legacy.example \
        > "$DST"/deploy/monitor-agent.service.legacy.example
    chmod 755 "$DST"/*.py
    echo "[2/5] 已完成覆盖安装到 $DST"

    if check_python; then
        "$PYTHON_BIN" -m py_compile "$DST"/*.py
        cleanup_pycache "$DST"
        echo "[3/5] 语法检查通过（已清理安装目录的 __pycache__）"
        check_psutil
    else
        echo "[3/5] 警告：跳过语法检查（未找到可用的 python3）"
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
    prune_backups
    echo
    echo "快速验证："
    echo "  python3 $DST/main.py --selftest"
    echo "  python3 $DST/main.py"
    echo
    if [[ -d "$BAK" ]]; then
        echo "原文件备份在："
        echo "  $BAK"
    else
        echo "原文件备份：本次为全新安装，未创建备份目录"
    fi
    if [[ "$PSUTIL_OK" -eq 0 ]]; then
        echo
        echo "!! 依赖提醒：$PYTHON_BIN 缺少 psutil，服务启动后会反复重启（ModuleNotFoundError）。"
        echo "   请执行以下命令安装依赖后重启服务："
        echo "     \"$PYTHON_BIN\" -m pip install -r $DST/requirements.txt"
    fi
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

# 按主机启用服务/日志监控（三者任选其一，详见 README.md）
# MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
# MONITOR_SERVICES=[{"name":"nginx","process_names":["nginx"],"port":80}]
# MONITOR_LOG_JOBS=[{"name":"nginx_error","path":"/var/log/nginx/error.log","patterns":[["connect\\(\\) failed","NGINX_UPSTREAM_FAIL","Nginx 后端网关异常"]]}]
# 非标准安装位置可用 paths 数组按序探测，如 paths:["/var/log/nginx/error.log","/www/server/nginx/logs/error.log"]

# 采集与告警参数（可选，默认值见 README.md）
# MONITOR_INTERVAL=10
# LOG_SCAN_INTERVAL=10
# ALERT_COOLDOWN=300
# ALERT_CONSECUTIVE=3
# 阈值覆盖示例（默认见 README.md）
# CPU_PERCENT_WARNING=85
# CPU_PERCENT_CRITICAL=97
# ALERT_STATE_FILE=/var/lib/monitor-agent/alert-state.json
EOF
    chmod 600 "$ENV_FILE"
    echo "  [OK] 已生成密钥模板: $ENV_FILE（请填入 DINGTALK_WEBHOOK 后重启服务）"
}

install_systemd() {
    require_root
    if ! systemd_available; then
        echo "错误：未检测到 systemd（PID 1 不是 systemd 或缺少 systemctl）" >&2
        echo "       本机请改用: sudo bash $0 files 后手动启动，或改用 openrc/sysvinit 方案" >&2
        exit 1
    fi
    if ! check_python; then
        exit 1
    fi
    install_files
    if [[ "$PSUTIL_OK" -eq 0 ]]; then
        echo
        echo "=============================================================="
        echo "!! 警告：未检测到 Python 依赖 psutil，服务可能立即崩溃并反复重启。"
        echo "   请先安装依赖，再执行 systemctl restart $SERVICE_NAME ："
        echo "     \"$PYTHON_BIN\" -m pip install -r $DST/requirements.txt"
        echo "=============================================================="
    fi
    echo
    echo "== 注册 systemd 开机自启 =="
    local sv template dst_esc py_esc
    sv=$(systemd_version)
    if [[ -n "$sv" && "$sv" -lt 235 ]]; then
        template="$SRC/deploy/monitor-agent.service.legacy.example"
        echo "  [OK] 检测到旧版 systemd ($sv)，使用兼容模板（手工创建运行目录）"
    else
        template="$SRC/deploy/monitor-agent.service.example"
        echo "  [OK] 使用标准模板（systemd ${sv:-未知版本}）"
    fi
    dst_esc=$(sed_repl_escape "$DST")
    py_esc=$(sed_repl_escape "$PYTHON_BIN")
    sed -e "s|__MONITOR_DIR__|${dst_esc}|g" \
        -e "s|__PYTHON_BIN__|${py_esc}|g" \
        "$template" > "$UNIT_PATH"
    chmod 644 "$UNIT_PATH"
    echo "  [OK] 已安装服务单元: $UNIT_PATH（ExecStart: $PYTHON_BIN）"
    write_env_template
    systemctl daemon-reload
    systemctl enable "$SERVICE_NAME"
    # enable --now 对已处于 active 的单元是 no-op（升级场景旧进程仍在跑旧代码），
    # 因此这里显式区分 restart / start，保证“升级即重启加载新代码”
    local start_failed=0
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        echo "  [OK] 服务已在运行，执行 restart 以加载新代码"
        systemctl restart "$SERVICE_NAME" || start_failed=1
    else
        echo "  [OK] 服务未运行，执行 start"
        systemctl start "$SERVICE_NAME" || start_failed=1
    fi
    # 即使启动/重启失败也要先输出诊断信息（|| true 保证 set -e 下仍打印状态）
    systemctl --no-pager --lines=20 status "$SERVICE_NAME" || true
    if (( start_failed != 0 )); then
        echo "  [错误] 服务启动/重启失败，请按上面的状态输出与 journalctl -u $SERVICE_NAME 排查" >&2
        exit 1
    fi
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

confirm_uninstall() {
    # 卸载确认：MONITOR_AGENT_YES=1 或非交互终端（stdin 非 tty）时跳过；
    # 交互模式下必须输入 yes 才继续，默认不删除
    if [[ "${MONITOR_AGENT_YES:-0}" == "1" ]]; then
        echo "  [跳过确认] 已设置 MONITOR_AGENT_YES=1"
        return 0
    fi
    if [[ ! -t 0 ]]; then
        echo "  [跳过确认] 非交互环境（stdin 不是终端）"
        return 0
    fi
    local reply=""
    printf '  确认删除请输入 yes（其他任何输入都会取消）: '
    read -r reply || reply=""
    if [[ "$reply" == "yes" ]]; then
        return 0
    fi
    echo "  [取消] 输入不是 yes，未执行任何删除操作" >&2
    return 1
}

uninstall_all() {
    require_root
    guard_target
    local TARGET d
    TARGET=$(canon_path "$DST")
    echo "== 完全卸载 =="
    echo "  将要删除的绝对路径："
    echo "    $TARGET"
    echo "    $APP_ZIP"
    echo "    $UNIT_PATH"
    if ! confirm_uninstall; then
        echo "已取消卸载，未删除任何文件。" >&2
        exit 1
    fi
    # 删除前双保险：再次确认目标非空、非根目录、且不在系统目录黑名单中
    if [[ -z "$TARGET" || "$TARGET" == "/" ]]; then
        echo "错误：卸载目标为空或根目录，已中止" >&2
        exit 1
    fi
    for d in "${SYSTEM_DIR_BLACKLIST[@]}"; do
        if [[ "$TARGET" == "$d" ]]; then
            echo "错误：拒绝删除系统关键目录 $TARGET，已中止" >&2
            exit 1
        fi
    done
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
    rm -f "$UNIT_PATH"
    systemctl daemon-reload
    rm -f "$APP_ZIP"
    # ${TARGET:?} 空值双保险：变量为空时直接报错退出，绝不执行 rm -rf
    rm -rf "${TARGET:?refusing to remove empty target path}"
    echo "  [OK] 已删除服务单元、$TARGET 与 $APP_ZIP"
    echo "  [提示] 配置与运行数据（$ETC_DIR、/var/lib/monitor-agent）保留未删除"
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
