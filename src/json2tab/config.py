class Config:
    json_file = ""
    out_dir = ""
    identifiers = ()
    chunk_size = 0

    def __init__(self, json_file, out_dir, chunk_size):
        self.json_file = json_file
        self.out_dir = out_dir
        self.chunk_size = chunk_size
