#/bin/bash
#by yj,2017.11.22

alarm()
{
    msg=""
    for args in $@
    do
        msg="$msg,$args"
    done
    python send_email.py "${msg}" 1
}

day=`date -d "-2 hours" +"%Y%m%d"`
hour=`date -d "-2 hours" +"%H"`
statis_day=`date -d "-3 hours" +"%Y%m%d"`
statis_hour=`date -d "-3 hours" +"%H"`

original_req_dir=/user/appsearch_dev/feed_log/track/${day}/track-log_${day}_${hour}*lzo
original_resp_dir=/user/appsearch_dev/feed_log/article_show/${day}/artshow-log_${day}_${hour}*lzo
output_dir=yuanjun/penguin/${day}/${day}${hour}

hadoop fs -test -e $output_dir
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $output_dir
fi



hadoop fs -test -e $original_resp_dir
if [ $? -eq 0 ];then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=256 \
	-D mapred.reduce.tasks=256 \
	-D mapred.job.name=wx_app_yj_penguin_track \
	-D mapred.task.timeout=3600000 \
	-file penguin_decode_mapper.py \
	-mapper "python penguin_decode_mapper.py" \
    -input ${original_req_dir} ${original_resp_dir} \
	-output ${output_dir} \
	-inputformat KeyValueTextInputFormat
fi

if [[ $? != 0 ]]; then
    msg="penguin_track_get_wrong_at_m-r_step!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi

rm -rf data/penguin_${day}${hour}
rm -rf log/penguin_${day}${hour}
mkdir data/penguin_${day}${hour}
mkdir log/penguin_${day}${hour}
hadoop fs -get ${output_dir}/part* data/penguin_${day}${hour}
hadoop fs -rm -r ${output_dir}

for ((i=0; i<256; i++))
do
    k=$(printf "%03d\n" $i)
    python penguin_pingback.py data/penguin_${day}${hour}/part-00${k} 1>log/penguin_${day}${hour}/part-00${k}.log 2>log/penguin_${day}${hour}/part-00${k}.err &
done

rm -f log/one_hour_log_${statis_day}${statis_hour}.log log/one_hour_log_${statis_day}${statis_hour}.err
for ((i=0; i<256; i++))
do
    k=$(printf "%03d\n" $i)
	python penguin_log_statistics.py log/penguin_${statis_day}${statis_hour}/part-00${k}.log 1>>log/one_hour_log_${statis_day}${statis_hour}.log 2>log/one_hour_log_${statis_day}${statis_hour}.err
done
python final_static.py log/one_hour_log_${statis_day}${statis_hour}.log 1>>total_log.log 2>total_log.err
tmpwatch -afv 6 ./data
tmpwatch -afv 48 ./log