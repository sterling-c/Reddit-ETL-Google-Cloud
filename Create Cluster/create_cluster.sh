REGION="REGION"
NAME="CLUSTER_NAME"
ZONE="ZONE"
gcloud dataproc clusters create $NAME \
 --scopes=default \
 --region $REGION --zone $ZONE \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 200 \
  --num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 200 \
--metadata 'PIP_PACKAGES=pandas praw google-cloud-storage' \
--initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
--image-version 1.4