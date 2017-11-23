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

original_dir=/user/appsearch_dev/feed_log/track/${day}/track-log_${day}_${hour}*lzo
output_dir=yuanjun/temp_data/penguin_req/${day}/${day}${hour}

hadoop fs -test -e $output_dir
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $output_dir
fi

hadoop fs -test -e $original_dir
if [ $? -eq 0 ];then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=64 \
	-D mapred.reduce.tasks=64 \
	-D mapred.job.name=wx_app_yj_penguin_track \
	-D mapred.task.timeout=3600000 \
	-file penguin_decode_mapper.py \
	-mapper "python penguin_decode_mapper.py" \
    -input ${original_dir} \
	-output ${output_dir} \
	-inputformat KeyValueTextInputFormat
fi

if [[ $? != 0 ]]; then
    msg="penguin_track_get_wrong_at_m-r_step!"
	timestamp=`date +"%Y%m%d%H%M"`
	alarm ${msg} ${timestamp}
	exit -1
fi

rm -rf data/penguin_req_${day}${hour}
mkdir data/penguin_req_${day}${hour}
hadoop fs -get ${output_dir}/part* data/penguin_req_${day}${hour}
hadoop fs -rm -r ${output_dir}
rm -rf log/penguin_req_${day}${hour}
mkdir log/penguin_req_${day}${hour}
for ((i=0; i<64; i++))
do
    k=$(printf "%03d\n" $i)
    python penguin_pingback.py data/penguin_req_${day}${hour}/part-00${k} > log/penguin_req_${day}${hour}/part-00${k}.log 2>&1 &
done
del_day=`date -d "-4 hours" +"%Y%m%d"`
del_hour=`date -d "-4 hours" +"%H"`
del_dir=data/penguin_req_${del_day}${del_hour}
rm -rf ${del_dir}
