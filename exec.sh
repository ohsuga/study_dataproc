#!/bin/bash

CLUSTER_NAME='dataproc01'
GCS_BUCKET='output20161121'

# Scala プログラムをビルド
sbt package

# クラスタを作成
gcloud beta dataproc clusters create ${CLUSTER_NAME} --bucket ${GCS_BUCKET} \
       --zone asia-east1-a \
       --master-machine-type n1-standard-4 \
       --worker-machine-type n1-standard-4 \
       --num-workers 5

# Spark ジョブを実行
gcloud beta dataproc jobs submit spark \
       --cluster ${CLUSTER_NAME} \
       --class net.refabrik.studyDataproc.CalcRecommendItems \
       --properties spark.dynamicAllocation.enabled=false,spark.executor.cores=3,spark.executor.memory=2g,spark.executor.instances=9 \
       --jars ./target/scala-2.11/calc-recommend-items_2.11-0.1.jar

# クラスタを削除
gcloud beta dataproc clusters delete -q ${CLUSTER_NAME}
