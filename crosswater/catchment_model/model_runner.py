from pathlib import Path
import shutil

import tables

from crosswater.read_config import read_config


class ModelRunner(object):
    """Run all catchment models"""
    def __init__(self, config_file):
        """
        """
        config = read_config(config_file)
        self.input_file_name = config['catchment_model']['hdf_input_path']
        self.tmp_path = Path(config['catchment_model']['tmp_path'])
        self.layout_xml_path = Path(
            config['catchment_model']['layout_xml_path'])
        self.output_path = config['catchment_model']['output_path']
        self.number_of_workers = config['catchment_model']['number_of_workers']
        self.program_path = Path(__file__).parents[2] / Path('program')
        self.input_txt_name = 'input.txt'
        self._make_template_name()
        self._open_files()

    def _make_template_name(self):
        self.layout_name_template = '{stem}_{{id}}{suffix}'.format(
            stem=self.layout_xml_path.stem,
            suffix=self.layout_xml_path.suffix)


    def _prepare_tmp(self):
        """Create tmp path and worker patgs. Copy executable.
        """
        if self.tmp_path.exists():
            shutil.rmtree(str(self.tmp_path))
        self.tmp_path.mkdir()
        self.worker_paths = []
        for worker in range(self.number_of_workers):
            worker_path = self.tmp_path / Path('worker_{}'.format(worker))
            self.worker_paths.append(worker_path)
            shutil.copytree(str(self.program_path), str(worker_path))

    def _open_files(self):
        """Open HDF5 input and out files.
        """
        self.hdf_input = tables.open_file(self.input_file_name, mode='r')
        self.hdf_output = tables.open_file(self.output_path, mode='w',
                                           title='Crosswater results')

    def _close_files(self):
        """Open HDF5 in put and out files.
        """
        self.hdf_input.close()
        self.hdf_output.close()

    def make_input(self, id_, worker_path):
        """Create input files.
        """
        group = self.hdf_input.get_node('/', 'catch_{}'.format(id_))
        self._make_layout(id_, worker_path, group)
        self._make_time_varying_input(id_, worker_path, group)

    def _make_layout(self, id_, worker_path, group):
        """Create the XML file for the model layout.
        """
        parameters_table= group._f_get_child('parameters')
        parameters = {row['name'].decode('ascii'): row['value']
                      for row in parameters_table}
        with open(str(self.layout_xml_path))as fobj:
            layout_template = fobj.read()
        layout = layout_template.format(id=id_,
                                        input_file_name=self.input_txt_name,
                                        **parameters)
        layout_file_name = Path(self.layout_name_template.format(id=id_))
        model_layout_path = worker_path / layout_file_name
        with open(str(model_layout_path), 'w') as fobj:
            fobj.write(layout)

    def _make_time_varying_input(self, id_, worker_path, group):
        """Create the text file for the input of T, P, an Q.
        """
        input_table = group._f_get_child('input')
        txt_input_path = worker_path / Path(self.input_txt_name)
        with open(str(txt_input_path), 'w') as fobj:
            fobj.write('step\tT\tP\tQ\tEmptymeas\n')
            for step, row in enumerate(input_table):
                fobj.write('{step}\t{T}\t{P}\t{Q}\tN/A\n'.format(
                    step=step,T=row['temperature'], P=row['precipitation'],
                    Q=row['discharge']))

    def store_output(sef):
        group = h5_file.create_group('/', 'catch_{}'.format(id_),
                                             'catchment {}'.format(id_))
        table = h5_file.create_table(group, 'parameters', Parameters,
                                             'constant parameters')
    def run_all(self):
        self._prepare_tmp()





