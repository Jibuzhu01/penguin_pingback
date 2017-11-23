#/bin/bash
#by yj,2017.11.22

day=`date -d "-2 hours" +"%Y%m%d"`
hour=`date -d "-2 hours" +"%H"`


rm -rf log/penguin_resp_${day}${hour}
mkdir log/penguin_resp_${day}${hour}
for ((i=0; i<256; i++))
do
    k=$(printf "%03d\n" $i)
    python penguin_pingback.py data/penguin_resp_${day}${hour}/part-00${k} 1>log/penguin_resp_${day}${hour}/part-00${k}.log 2>log/penguin_resp_${day}${hour}/part-00${k}.err &
done
statis_day=`date -d "-3 hours" +"%Y%m%d"`
statis_hour=`date -d "-3 hours" +"%H"`
for ((i=0; i<256; i++))
do
    k=$(printf "%03d\n" $i)
	python penguin_log_statistics.py log/penguin_resp_${statis_day}${statis_hour}/part-00${k}.log 1>>log/one_hour_log_${statis_day}${statis_hour}.log 2>log/one_hour_log_${statis_day}${statis_hour}.err
done
python final_static.py log/one_hour_log_${statis_day}${statis_hour}.log 1>>total_log.log 2>total_log.err
tmpwatch -afv 6 ./data
tmpwatch -afv 48 ./log