#/bin/bash
#by yj,2017.9.6

alarm()
{
    msg=""
    for args in $@
    do
        msg="$msg,$args"
    done
    python send_email.py "${msg}" 1
}

day=`date -d "-1 hours" +"%Y%m%d"`
hour=`date -d "-1 hours" +"%H"`

original_dir=/user/appsearch_dev/feed_log/article_show/${day}/artshow-log_${day}_${hour}*lzo
output_dir=yuanjun/temp_data/penguin_resp/${day}/${day}${hour}

hadoop fs -test -e $output_dir
if [ $? -eq 0 ]; then
    hadoop fs -rm -r $output_dir
fi

hadoop fs -test -e $original_dir
if [ $? -eq 0 ];then
    hadoop org.apache.hadoop.streaming.HadoopStreaming \
	-D mapred.map.tasks=16 \
	-D mapred.reduce.tasks=16 \
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


hadoop fs -getmerge ${output_dir}/part* data/penguin_resp_${day}${hour}
hadoop fs -rm -r ${output_dir}
cat data/penguin_resp_${day}${hour} | python penguin_pingback.py
del_day=`date -d "-4 hours" +"%Y%m%d"`
del_hour=`date -d "-4 hours" +"%H"`
del_file=data/penguin_resp_${del_day}${del_hour}
rm -f ${del_file}
