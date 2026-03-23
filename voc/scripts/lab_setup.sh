echo "IN LAB SETUP AT $(date)"

# Check if the lab overrides the default logic
if [ -f /voc/private/python/lab_setup.py ]; then
    /voc/private/python/lab_setup.py
    RC=$?
else
    /voc/scripts/python/lab_setup.py
    RC=$?
fi
echo "FINISHED LAB SETUP AT $(date)"
exit $RC
