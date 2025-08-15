from pathlib import Path


class Storage:
    def __init__(self, at: str):
        Path(at).mkdir(exist_ok=True, parents=True)
        self.path = at

    def new():
        pass
