ps -ef | grep Python | grep -v grep | awk '{print $2}' | xargs kill
