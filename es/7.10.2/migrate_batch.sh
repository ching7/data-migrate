#!/bin/bash

# 配置参数（按照实际情况调整）
SOURCE_ES="http://172.30.32.170:9220"
SOURCE_ES_USER=""       # 源ES用户名，无密码填空 ""
SOURCE_ES_PASSWORD="" # 源ES密码，无密码填空 ""
TARGET_ES="http://172.30.32.170:9221"
TARGET_ES_USER=""       # 目标ES用户名，无密码填空 ""
TARGET_ES_PASSWORD="" # 目标ES密码，无密码填空 ""
INDEX="aicc-test-seatcallrecord143" #需要迁移的索引
TIME_FIELD="callStartTime"  # 时间字段（按实际字段调整）
BATCH_DAYS=30  # 每批迁移30天数据
TOTAL_MONTHS=2  # 总迁移6个月数据
END_DATE="2025-06-15 23:59:59"  # 最新数据时间


# es7.X版本，固定
TYPE="_doc" # 索引类型字段
# 日志目录
BASE_DIR="./logs"
LOG_FILE="$BASE_DIR/migration_batch.log"
LOGSTASH_CONF_TEMPLATE="./logstash_template.conf"
LOGSTASH_CONF="./logstash.conf"

# 初始化时间范围（从当前时间倒推半年）
# END_DATE=$(date +"%Y-%m-%d 23:59:59")
START_DATE=$(date -d "-$TOTAL_MONTHS months" +"%Y-%m-%d 00:00:00")
# 时间戳
START_DATE_TS=$(date -d "$START_DATE" +%s)
TOTAL_COUNT=0
# 检查依赖工具是否存在
check_dependency() {
    if ! command -v $1 &> /dev/null; then
        echo "错误: 工具 '$1' 未安装。请先安装 $2"
        exit 1
    fi
}

# 检查必要工具
check_dependency "curl" "curl (通常已预装或使用 brew install curl)"
check_dependency "jq" "jq (brew install jq)"
check_dependency "docker" "Docker (https://www.docker.com/)"
check_dependency "docker compose" "Docker Compose (通常随Docker一起安装)"
check_dependency "wc" "coreutils (通常已预装)"
# 初始化日志
mkdir -p "$BASE_DIR"
touch "$LOG_FILE"
echo "===== 迁移批次记录开始（$(date)） =====" >> $LOG_FILE
echo "总时间范围：$START_DATE 至 $END_DATE，每批 $BATCH_DAYS 天" >> $LOG_FILE
# 导出源索引映射
echo "===== 开始导出源索引映射 =====" >> $LOG_FILE
MAPPING_FILE="$BASE_DIR/${INDEX}_mapping.json"
TMP_MAPPING_FILE="$BASE_DIR/${INDEX}_transformed_mapping.json"
touch $MAPPING_FILE $TMP_MAPPING_FILE
# 构建curl认证参数
build_auth_params() {
    local user="$1"
    local password="$2"
    if [ -n "$user" ] && [ -n "$password" ]; then
        echo "-u $user:$password"
    else
        echo ""
    fi
}
AUTH_PARAMS=$(build_auth_params "$SOURCE_ES_USER" "$SOURCE_ES_PASSWORD")
curl $AUTH_PARAMS -s -XGET "$SOURCE_ES/$INDEX/_mapping?pretty" -o $MAPPING_FILE
if [ $? -ne 0 ] || [ ! -s $MAPPING_FILE ]; then
    echo "映射导出失败或文件为空" >> $LOG_FILE
    exit 1
fi
echo "映射导出成功：$MAPPING_FILE" >> $LOG_FILE
# 添加JSON结构转换步骤
echo "===== 开始转换映射结构 =====" >> $LOG_FILE
# 使用变量替换硬编码的索引名和类型
jq --arg idx "$INDEX" '{"mappings": .[$idx].mappings | to_entries[0].value}' "$MAPPING_FILE" > "$TMP_MAPPING_FILE"
if [ $? -ne 0 ] || [ ! -s $TMP_MAPPING_FILE ]; then
    echo "JSON结构转换失败或文件为空" >> $LOG_FILE
    exit 1
fi
echo "映射结构转换成功：$TMP_MAPPING_FILE" >> $LOG_FILE
# 导入映射到目标索引
echo "===== 开始导入映射到目标索引 =====" >> $LOG_FILE
# 检查目标索引是否存在
INDEX_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "$TARGET_ES/$INDEX")
if [ "$INDEX_EXISTS" -eq 200 ]; then
    echo "目标索引 $INDEX 已存在." >> $LOG_FILE
else
    curl -s -XPUT "$TARGET_ES/$INDEX" -H 'Content-Type: application/json' -d @$TMP_MAPPING_FILE
    if [ $? -ne 0 ]; then
        echo "映射导入失败" >> $LOG_FILE
        exit 1
    fi
    echo "映射导入成功" >> $LOG_FILE
fi

# 初始化循环变量
current_end="$END_DATE"

while true; do
    # ===== 1. 计算当前批次的起始时间 =====
    current_end_ts=$(date -d "$current_end" +%s)
    current_start_ts=$((current_end_ts - BATCH_DAYS * 86400))
    
    if [ $current_start_ts -lt $START_DATE_TS ]; then
        current_start_ts=$START_DATE_TS
    fi
    
    current_start=$(date -d "@$current_start_ts" +"%Y-%m-%d %H:%M:%S")
    
    echo "===== 开始迁移批次：$current_start 至 $current_end =====" >> $LOG_FILE

    BATCH_ID=$(date -d "$current_start" +"%Y%m%d%H%M%S")
    BATCH_STATS_NAME="$INDEX-batch_stats_$BATCH_ID.json"
    # logstash本身为宿主机目录
    BATCH_STATS_FILE=$BASE_DIR/$BATCH_STATS_NAME
    # ===== 4. 记录迁移统计 =====
    if [ ! -f "$BATCH_STATS_FILE" ]; then
        touch "$BATCH_STATS_FILE"
        echo "初始化迁移统计文件" >> $LOG_FILE
    fi
    if [ -f "$BATCH_STATS_FILE" ]; then
        > "$BATCH_STATS_FILE"
        echo "$(date) 已清空历史统计文件" >> $LOG_FILE
    fi
    chmod -R 777 $BASE_DIR
    # ===== 2. 替换Logstash配置文件变量 =====
    # 构建Logstash配置的条件变量
    IF_SOURCE_ES_USER_DEFINED=""
    IF_SOURCE_ES_PASSWORD_DEFINED=""
    IF_TARGET_ES_USER_DEFINED=""
    IF_TARGET_ES_PASSWORD_DEFINED=""
    
    if [ -n "$SOURCE_ES_USER" ] && [ -n "$SOURCE_ES_PASSWORD" ]; then
        IF_SOURCE_ES_USER_DEFINED=""
        IF_SOURCE_ES_PASSWORD_DEFINED=""
    else
        IF_SOURCE_ES_USER_DEFINED="#"
        IF_SOURCE_ES_PASSWORD_DEFINED="#"
    fi
    
    if [ -n "$TARGET_ES_USER" ] && [ -n "$TARGET_ES_PASSWORD" ]; then
        IF_TARGET_ES_USER_DEFINED=""
        IF_TARGET_ES_PASSWORD_DEFINED=""
    else
        IF_TARGET_ES_USER_DEFINED="#"
        IF_TARGET_ES_PASSWORD_DEFINED="#"
    fi
    
    sed -e "s|%{START_TIME}|$current_start|g" \
        -e "s|%{END_TIME}|$current_end|g" \
        -e "s|%{BATCH_STATS_FILE}|$BATCH_STATS_NAME|g" \
        -e "s|%{SOURCE_ES}|$SOURCE_ES|g" \
        -e "s|%{SOURCE_ES_USER}|$SOURCE_ES_USER|g" \
        -e "s|%{SOURCE_ES_PASSWORD}|$SOURCE_ES_PASSWORD|g" \
        -e "s|%{INDEX}|$INDEX|g" \
        -e "s|%{TARGET_ES}|$TARGET_ES|g" \
        -e "s|%{TARGET_ES_USER}|$TARGET_ES_USER|g" \
        -e "s|%{TARGET_ES_PASSWORD}|$TARGET_ES_PASSWORD|g" \
        -e "s|%{TYPE}|$TYPE|g" \
        -e "s|%{IF_SOURCE_ES_USER_DEFINED}|$IF_SOURCE_ES_USER_DEFINED|g" \
        -e "s|%{IF_SOURCE_ES_PASSWORD_DEFINED}|$IF_SOURCE_ES_PASSWORD_DEFINED|g" \
        -e "s|%{IF_TARGET_ES_USER_DEFINED}|$IF_TARGET_ES_USER_DEFINED|g" \
        -e "s|%{IF_TARGET_ES_PASSWORD_DEFINED}|$IF_TARGET_ES_PASSWORD_DEFINED|g" \
        "$LOGSTASH_CONF_TEMPLATE" > "$LOGSTASH_CONF"    

    # ===== 3. 启动Logstash迁移当前批次 =====
    BATCH_START_TS=$(date +%s)
    docker compose up -d logstash
    CONTAINER_ID=$(docker compose ps -q logstash)
    echo "Logstash容器ID: $CONTAINER_ID，开始迁移..." >> $LOG_FILE
    
    # 等待迁移完成
    docker wait "$CONTAINER_ID"
    LOGSTASH_EXIT_CODE=$?
    BATCH_END_TS=$(date +%s)
    BATCH_DURATION=$((BATCH_END_TS - BATCH_START_TS))
    
    if [ $LOGSTASH_EXIT_CODE -ne 0 ]; then
        echo "批次 $current_start 至 $current_end 迁移失败，退出码: $LOGSTASH_EXIT_CODE" >> $LOG_FILE
        exit $LOGSTASH_EXIT_CODE
    fi
    BATCH_COUNT=$(wc -l < "$BATCH_STATS_FILE")
    TOTAL_COUNT=$((TOTAL_COUNT + BATCH_COUNT))
    if [ $BATCH_DURATION -lt 0 ]; then
        echo "批次 $current_start 至 $current_end 耗时为负：$BATCH_DURATION 秒，请检查时间逻辑。" >> $LOG_FILE
    fi
    
    echo "批次 $current_start 至 $current_end：迁移 $BATCH_COUNT 条数据，耗时 $BATCH_DURATION 秒，结果：$BATCH_STATS_FILE" >> $LOG_FILE
    
    # ===== 5. 随机抽查验证 =====
    if [ $BATCH_COUNT -gt 0 ]; then
        SUCCESS=0
        FAILED=0

        # 提取当前批次的 document_id 并随机抽取最多 10 个
        RANDOM_IDS=$(jq -r '.document_id' "$BATCH_STATS_FILE" | shuf | head -10)
        ID_COUNT=$(echo "$RANDOM_IDS" | wc -l)

        echo "[验证开始] 批次 $current_start 至 $current_end，共需验证 $ID_COUNT 个ID" >> $LOG_FILE

        while read -r id; do
            VAL_START=$(date +%s)
            if curl -s "$TARGET_ES/$INDEX/$TYPE/$id" | jq -e '.found == true' > /dev/null; then
                echo "[验证成功] ID $id 耗时 $(( $(date +%s) - VAL_START )) 秒" >> $LOG_FILE
                SUCCESS=$((SUCCESS + 1))
            else
                echo "[验证失败] ID $id 耗时 $(( $(date +%s) - VAL_START )) 秒" >> $LOG_FILE
                FAILED=$((FAILED + 1))
            fi
        done <<< "$RANDOM_IDS"

        if [[ $((SUCCESS + FAILED)) -gt 0 ]]; then
            RATE=$(( SUCCESS * 100 / (SUCCESS + FAILED) ))
            echo "[验证统计] 成功 $SUCCESS 条，失败 $FAILED 条，成功率 $RATE%" >> $LOG_FILE
        else
            echo "[验证警告] 无有效验证样本（可能日志格式不包含 document_id）" >> $LOG_FILE
        fi
    else
        echo "批次 $current_start 至 $current_end：无数据迁移，跳过验证步骤" >> $LOG_FILE
    fi
    # ===== 6. 判断是否迁移完成 =====
    if [ $current_start_ts -le $START_DATE_TS ]; then
        echo "=====总时间范围：$START_DATE 至 $END_DATE 所有批次迁移完成（$(date)），总计迁移 $TOTAL_COUNT 条数据 =====" >> $LOG_FILE
        exit 0
    fi
    
    # ===== 7. 更新为下一批次 =====
    current_end=$(date -d "@$((current_start_ts - 1))" +"%Y-%m-%d %H:%M:%S")
done