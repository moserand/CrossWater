import os
from pathlib import Path
import random
import shutil
import subprocess
import time
from threading import Thread

import tables

from crosswater.read_config import read_config
from crosswater.tools.hdf5_helpers import find_ids
from crosswater.tools.path_helper import ChDir


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
        self._make_template_name()
        self._open_files()
        self._prepare_tmp()

    def _make_template_name(self):
        """Create template for the name of the layout file.
        """
        self.layout_name_template = '{stem}_{{id}}{suffix}'.format(
            stem=self.layout_xml_path.stem,
            suffix=self.layout_xml_path.suffix)

    def _prepare_tmp(self):
        """Create tmp path and worker paths. Copy executable.
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

        def run_all(self):
            self._prepare_tmp()

    def run_all(self):
        all_ids = iter(find_ids(self.hdf_input))
        free_paths = self.worker_paths[:]
        active_workers = {}
        done = False
        while True:
            for index, path in enumerate(free_paths):
                try:
                    id_ = next(all_ids)
                except StopIteration:
                    done = True
                    break
                print(id_)
                worker = Worker(id_, path, self.hdf_input,
                                self.layout_xml_path,
                                self.layout_name_template)
                worker.start()
                active_workers[path] = worker
            if done:
                break
            free_paths = []
            for path, worker in active_workers.items():
                if not worker.is_alive():
                    free_paths.append(path)
        for worker in active_workers.values():
            worker.join()


class Worker(Thread):
    """One model run.
    """
    def __init__(self, id_, path, hdf_input, layout_xml_path,
                 layout_name_template):
        super().__init__()
        self.id = id_
        self.path = path
        self.done = False
        self.hdf_input = hdf_input
        self.layout_xml_path = layout_xml_path
        self.layout_name_template = layout_name_template
        self.input_txt_name = 'input.txt'

    def _make_input(self):
        """Create input files.
        """
        group = self.hdf_input.get_node('/', 'catch_{}'.format(self.id))
        self._make_layout(group)
        self._make_time_varying_input(group)

    def _make_layout(self, group):
        """Create the XML file for the model layout.
        """
        parameters_table= group._f_get_child('parameters')
        parameters = {row['name'].decode('ascii'): row['value']
                      for row in parameters_table}
        with open(str(self.layout_xml_path))as fobj:
            layout_template = fobj.read()
        layout = layout_template.format(id=self.id,
                                        input_file_name=self.input_txt_name,
                                        **parameters)
        layout_file_name = Path(self.layout_name_template.format(id=self.id))
        model_layout_path = self.path / layout_file_name
        with open(str(model_layout_path), 'w') as fobj:
            fobj.write(layout)

    def _make_time_varying_input(self, group):
        """Create the text file for the input of T, P, an Q.
        """
        input_table = group._f_get_child('inputs')
        txt_input_path = self.path / Path(self.input_txt_name)
        with open(str(txt_input_path), 'w') as fobj:
            fobj.write('step\tT\tP\tQ\tEmptymeas\n')
            for step, row in enumerate(input_table):
                fobj.write('{step}\t{T}\t{P}\t{Q}\tN/A\n'.format(
                    step=step,T=row['temperature'], P=row['precipitation'],
                    Q=row['discharge']))

    def _execute(self):
        with ChDir(str(self.path)):
            try:
                subprocess.check_call(
                    ['server',
                     self.layout_name_template.format(id=self.id),
                     'RUN',
                     'out_{}.txt'.format(self.id)])
            except subprocess.CalledProcessError as err:
                print('error running model for ID', self.id)
                print('exits staus:', err.returncode)

    def run(self):
        self._make_input()
        self._execute()
        time.sleep(random.random())
        self.done = True

