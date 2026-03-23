echo "IN LAB GRADE AT $(date)"

# Check if the lab overrides the default logic
if [ -f /voc/private/python/grade.py ]; then
    /voc/private/python/grade.py $vocareumReportFile $vocareumGradeFile
    RC=$?
else
    /voc/scripts/python/grade.py $vocareumReportFile $vocareumGradeFile
    RC=$?
fi
echo "FINISHED LAB GRADE AT $(date)"
exit $RC
