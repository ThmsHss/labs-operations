echo "IN LAB END AT $(date)"

# Check if the lab overrides the default logic
if [ -f /voc/private/python/lab_end.py ]; then
    /voc/private/python/lab_end.py
    RC=$?
else
    /voc/scripts/python/lab_end.py
    RC=$?
fi
echo "FINISHED LAB END AT $(date)"
exit $RC
