# Note that on Mac OS, you may have to copy and paste this script to run.
sudo docker build --progress=plain --build-arg xtract_db=$XTRACT_DB --build-arg xtract_pass=$XTRACT_PASS --build-arg aws_access=$aws_access --build-arg aws_secret=$aws_secret --build-arg globus_client=$GLOBUS_FUNCX_CLIENT --build-arg globus_secret=$GLOBUS_FUNCX_SECRET -t crawler-worker .
