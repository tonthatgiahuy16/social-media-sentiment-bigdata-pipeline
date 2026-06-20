#!/bin/bash
# Hadoop Streaming Word Count - alternative to Java MapReduce
# Run via Hadoop streaming API (no compilation needed)

HDFS_INPUT="/sentiment/raw/sentiment140_full.csv"
HDFS_OUTPUT="/sentiment/wordcount_streaming"

echo "=== Hadoop Streaming Word Count ==="

# Mapper: extract words from tweet text (field 6, 0-indexed field 5)
cat << 'MAPPER_SCRIPT' > /tmp/mapper.py
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    try:
        fields = line.strip().split(",", 6)
        if len(fields) >= 6:
            text = fields[5].lower()
            text = text.replace("http://", "").replace("https://", "")
            text = "".join(c if c.isalpha() or c.isspace() else " " for c in text)
            for word in text.split():
                if len(word) > 2:
                    print(f"{word}\t1")
    except:
        pass
MAPPER_SCRIPT

# Reducer: sum word counts
cat << 'REDUCER_SCRIPT' > /tmp/reducer.py
#!/usr/bin/env python3
import sys
current_word = None
count = 0
for line in sys.stdin:
    word, cnt = line.strip().split("\t", 1)
    if current_word == word:
        count += int(cnt)
    else:
        if current_word:
            print(f"{current_word}\t{count}")
        current_word = word
        count = int(cnt)
if current_word:
    print(f"{current_word}\t{count}")
REDUCER_SCRIPT

chmod +x /tmp/mapper.py /tmp/reducer.py

# Run Hadoop streaming
docker exec namenode bash -c "
    hdfs dfs -rm -r $HDFS_OUTPUT 2>/dev/null || true

    yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -files /tmp/mapper.py,/tmp/reducer.py \
        -mapper 'python3 mapper.py' \
        -reducer 'python3 reducer.py' \
        -input $HDFS_INPUT \
        -output $HDFS_OUTPUT \
        -numReduceTasks 2 \
        -D mapreduce.job.reduces=2
"

echo "=== Streaming Word Count complete ==="
echo "Results:"
docker exec namenode hdfs dfs -cat "$HDFS_OUTPUT/part-00000" | sort -k2 -nr | head -20
