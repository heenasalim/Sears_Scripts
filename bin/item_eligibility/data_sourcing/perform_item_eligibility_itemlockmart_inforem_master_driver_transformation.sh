
echo "--------------------------------------------------------------"
echo " Writing to Kettle Log File"
echo "--------------------------------------------------------------"
echo $0 $*

env

logFile="/logs/hdidrp/item_eligibilitY_kettle_demo.log"
echo "--------------------------------------------------------------" >> ${logFile}
echo " Writing to ${logFile} Log File"
echo "--------------------------------------------------------------" >> ${logFile}

echo $0 $* >> ${logFile}

env >>  ${logFile}

echo "--------------------------------------------------------------" >> ${logFile}
