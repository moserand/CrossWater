from pathlib import Path
import shutil
from crosswater.read_config import read_config


class ModelRunner(object):
    """Run all catchment models"""
    def __init__(self, config_file):
        """
        """
        config = read_config(config_file)
        self.input_file_name = config['catchment_model']['hdf_input_path']
        self.tmp_path = Path(config['catchment_model']['tmp_path'])
        self.layout_xml_pathh = config['catchment_model']['layout_xml_path']
        self.output_path = config['catchment_model']['output_path']
        self.number_of_workers = config['catchment_model']['number_of_workers']
        self.program_path = Path(__file__).parents[2] / Path('program')

    def prepare_tmp(self):
        if self.tmp_path.exists():
            shutil.rmtree(str(self.tmp_path))
        self.tmp_path.mkdir()
        self.worker_paths = []
        for worker in range(self.number_of_workers):
            worker_path = self.tmp_path / Path('worker_{}'.format(worker))
            self.worker_paths.append(worker_path)
            shutil.copytree(str(self.program_path), str(worker_path))



    def run_all(self):
        self.prepare_tmp()





