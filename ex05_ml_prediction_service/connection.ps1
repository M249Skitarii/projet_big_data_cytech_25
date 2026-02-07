# This script reset SSH keys for an address.
# Use to reconnect to container when it has rebooted
# port 2222 is used for the training on spark-master
# port 2223 is used for the streamlit interface


ssh-keygen -R "[localhost]:2222"
ssh -p 2222 root@localhost #mdp: "1"

# if connection refused sshd might not have started, wait for the container to start
# if container has started, try docker exec -it spark-master /usr/sbin/sshd