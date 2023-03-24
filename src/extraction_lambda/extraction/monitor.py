

class Monitor:
    def __init__(self,s3_bucket_name):
        self.s3_bucket_name = s3_bucket_name
        self.state = {"tup_inserted":0,"tup_updated":0, "tup_deleted":0}
        
    def has_changed(self):
        return True
    
    def has_state_changed(self,new_state):
        return self.state != new_state