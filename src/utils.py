from .taskminer import task_miner
from .createnotes import create_notes
from .updatenotes import update_notes
from .noteminer import mine_notes
from .archival import archival_process

class Driver():

    def __init__(self, **kwargs) -> None:
        self.tk = kwargs['token']
        self.vpath = kwargs['vault_path']
    
    def mineNotes(self):
        mine_notes(self.vpath)

    def _build_step(func):
        def wrapper(self, *args, **kwargs):
            print('-'*10, f"starting execution of {func.__name__}", '-'*10, '\n')
            result = func(self, *args, **kwargs)
            self.mineNotes()
            print('-'*10, f"completed execution of {func.__name__}", '-'*10, '\n')
            return result
        return wrapper

    @_build_step
    def mineTasks(self):
        task_miner(self.tk)
    
    @_build_step
    def createNotes(self):
        create_notes()

    @_build_step
    def updateNotes(self):
        update_notes()

    @_build_step
    def archivalProcess(self):
        archival_process(self.vpath)


    def execute(self):
        self.mineTasks()
        self.createNotes()
        self.updateNotes()
        self.archivalProcess()

        return True
