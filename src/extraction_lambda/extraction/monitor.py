
# get update from db (state: inserted,updated,deleted)

# update log file and compare state(prev vs new)

# return T if there's any update or F

class Monitor:
    def __init__(self,s3_bucket_name):
        self.s3_bucket_name = s3_bucket_name
        self.state = {"tup_inserted":0,"tup_updated":0, "tup_deleted":0} 
        d
    def has_changed(self):
        return True
    
    def has_state_changed(self,new_state):
        return self.state != new_state
    
    def get_db_stats(self):
        # connect to db
        # .....get data & format it
        