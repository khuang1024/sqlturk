#! /bin/bash

for j in `seq 0 7`
do
    for i in `seq 2 10`
    do
        java -XX:-UseGCOverheadLimit -Xmx32G -XX:+UseParallelOldGC -jar experiment.jar iu world $j $i 200
        pid="${!}"
        wait $pid
        echo "Succeed: IU world query#$j top#$i." >> log.txt
        sleep 2
        java -XX:-UseGCOverheadLimit -Xmx32G -XX:+UseParallelOldGC -jar experiment.jar fd world $j $i 200
        rc=$?
        pid="${!}"
        wait $pid
        if [[ $rc != 0 ]]; then
            echo "Failed: FD world query#$j top#$i." >> log.txt
            break
        else
            echo "Succeeded: FD world query#$j top#$i." >> log.txt
        fi
        sleep 2
        java -XX:-UseGCOverheadLimit -Xmx32G -XX:+UseParallelOldGC -jar experiment.jar fdp world $j $i 200
        #java -XX:-UseGCOverheadLimit -Xmx8G -XX:+UseParallelOldGC -XX:ParallelGCThreads=4 -jar experiment.jar fdp world 1 $i
        rc=$?
        pid="${!}"
        wait $pid
        if [[ $rc != 0 ]]; then
            echo "Failed: FD+ world query#$j top#$i." >> log.txt
            break
        else
            echo "Succeeded: FD+ world query#$j top#$i." >> log.txt
        fi
        sleep 2
    done
done
