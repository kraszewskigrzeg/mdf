class Manager:
    
    def __init__(self, session, auto_save=True):
        self.s = session
        self.auto_save = auto_save
        
    def save(self, objs: list=None):
        if objs:
            self.s.add_all(objs)
        self.s.commit()
        
    def __len__(self, ):
        return len(self.get())