#/bin/bash
#by yj,2017.11.22

rm -f log/one_hour_log_2017112410.log log/one_hour_log_2017112410.err
for ((i=0; i<256; i++))
do
    k=$(printf "%03d\n" $i)
	python penguin_log_statistics.py log/penguin_2017112410/part-00${k}.log 1>>log/one_hour_log_2017112410.log 2>log/one_hour_log_2017112410.err
done
python final_static.py log/one_hour_log_2017112410.log 1>>total_log.log 2>total_log.err