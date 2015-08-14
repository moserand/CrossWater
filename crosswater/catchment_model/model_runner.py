"""Run all catchment models.

Calls external executable.
"""

from pathlib import Path
import random
import shutil
import subprocess
import time
from timeit import default_timer
from threading import Thread

import tables

from crosswater.read_config import read_config
from crosswater.tools.hdf5_helpers import find_ids
from crosswater.tools.path_helper import ChDir


class ModelRunner(object):
    """Run all catchment models"""
    # pylint: disable=too-few-public-methods
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
        self.worker_paths = []
        shutil.copytree(str(self.program_path), str(self.tmp_path))
        for worker in range(self.number_of_workers):
            worker_path = self.tmp_path / Path('worker_{}'.format(worker))
            worker_path.mkdir()
            self.worker_paths.append(worker_path)


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

    @staticmethod
    def _read_parameters(group):
        """Read all parameters.
        """
        # pylint: disable=protected-access
        parameters_table = group._f_get_child('parameters')
        return {row['name'].decode('ascii'): row['value'] for row in
                parameters_table}

    @staticmethod
    def _read_inputs(group):
        """Read whole input table.
        """
        # pylint: disable=protected-access
        parameters_table = group._f_get_child('inputs')
        return parameters_table[:]

    def _read_parameters_inputs(self, id_):
        """Read parameters and inputs for gibe ID.
        """
        group = self.hdf_input.get_node('/', 'catch_{}'.format(id_))
        return self._read_parameters(group), self._read_inputs(group)

    def run_all(self):
        """Run all models.
        """

        with ChDir(str(self.tmp_path)):
            start = default_timer()
            all_ids = find_ids(self.hdf_input)
            nids = len(all_ids)
            all_ids = iter(all_ids)
            free_paths = self.worker_paths[:]
            active_workers = {}
            done = False
            counter = 0
            while True:
                for path in free_paths:
                    try:
                        id_ = next(all_ids)
                    except StopIteration:
                        done = True
                        break
                    counter += 1
                    duration = default_timer() - start
                    fraction = counter / nids
                    total_time = duration / fraction
                    print('{:7} {:7d} {:5.1f} {:5.1f} {:6.2f} % '.format(
                        id_, counter, duration, total_time,
                        fraction * 100), end='\r')
                    parameters, inputs = self._read_parameters_inputs(id_)
                    worker = Worker(id_, path, parameters, inputs,
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
        print()


class Worker(Thread):
    """One model run.
    """
    def __init__(self, id_, path, parameters, inputs, layout_xml_path,
                 layout_name_template):
        super().__init__()
        self.id = id_
        self.path = path
        self.parameters = parameters
        self.inputs = inputs
        self.layout_xml_path = layout_xml_path
        self.layout_name_template = layout_name_template
        self.input_txt_name = str(self.path / 'input.txt')
        self.daemaon = True

    def _make_input(self):
        """Create input files.
        """
        self._make_layout(self.parameters)
        self._make_time_varying_input(self.inputs)

    def _make_layout(self, parameters):
        """Create the XML file for the model layout.
        """
        # pylint: disable=star-args
        with open(str(self.layout_xml_path))as fobj:
            layout_template = fobj.read()
        layout = layout_template.format(id=self.id,
                                        input_file_name=self.input_txt_name,
                                        **parameters)
        layout_file_name = Path(self.layout_name_template.format(id=self.id))
        model_layout_path = self.path / layout_file_name
        with open(str(model_layout_path), 'w') as fobj:
            fobj.write(layout)

    def _make_time_varying_input(self, inputs):
        """Create the text file for the input of T, P, an Q.
        """
        txt_input_path = self.path / Path(self.input_txt_name)
        with open(str(txt_input_path), 'w') as fobj:
            fobj.write('step\tT\tP\tQ\tEmptymeas\n')
            for step, row in enumerate(inputs):
                fobj.write('{step}\t{T}\t{P}\t{Q}\tN/A\n'.format(
                    step=step, T=row['temperature'], P=row['precipitation'],
                    Q=row['discharge']))

    def _execute(self):
        """Run external program for catchment model.
        """
        input_path = self.path / self.layout_name_template.format(
            id=self.id)
        output_path = self.path / 'out_{}.txt'.format(self.id)
        try:
            _ = subprocess.check_output(
                ['server', str(input_path), 'RUN', str(output_path)],
                stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as err:
            print('error running model for ID', self.id)
            print('exits staus:', err.returncode)

    def run(self):
        """Run thread.
        """
        self._make_input()
        self._execute()

