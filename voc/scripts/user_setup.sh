echo "IN USER SETUP AT $(date)"

# Check if the lab overrides the default logic
if [ -f /voc/private/python/user_setup.py ]; then
    /voc/private/python/user_setup.py
    RC=$?
else
    /voc/scripts/python/user_setup.py
    RC=$?
fi
echo "FINISHED USER SETUP AT $(date)"
exit $RC
