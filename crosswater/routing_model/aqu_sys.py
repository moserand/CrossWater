"""
Aquasim system. 
Definition of variables, processes, compartments and links in the Aquasim format.

@author: moserand
"""

import fnmatch
import math
import numpy as np
import tables
from crosswater.read_config import read_config


def brace(*args):
    """Put all arguments in {} and return in string. If argument type is a list, every element is in {}.
    
        ['a', ['b', 'c']] --> '{a}{b}{c}'
    """
    args = flatten(args)
    L = ['{'+str(arg)+'}' for arg in args]
    s = ''.join(map(str, L))
    return s

def flatten(foo):
    """Flatten a list - lists in list dissolved.
    
        ['a', ['b', 'c']] --> ['a', 'b', 'c']
    """
    for x in foo:
        if hasattr(x, '__iter__') and not isinstance(x, str):
            for y in flatten(x):
                yield y
        else:
            yield x

def output(name, *args):
    """String or list of string are formatted to output with braces and name.
    
        [arg1, [arg2, arg3]] --> '{Name}{arg1}[Name]{arg2}[Name]{arg3}...'
    """
    args = flatten(args)
    L = ['{'+name+'}{'+arg+'}' for arg in args]
    s = ''.join(map(str,L))
    return s
    
def zipping(node, col1, col2):
    """Combine two columns of a HDF node to one list ordered by the position.
    
        '{col1[1]}{col2[2]}{col1[2]}{col2[2]}...'
    """
    table = node.read()
    arg = table[col1]
    value = table[col2]
    array = np.dstack((arg, value))
    array_1d = array.reshape((1,-1))
    return list(flatten(array_1d))

   
class VarSys(object):
    """Aquasim Variable System
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['output_aqu_compartment_path']
        self.k_bio = config['routing_model']['biodegradation_rate']
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            self._compart_names = self._compart_names()
            self.progvar = self.progvar()
            self.constvar = self.constvar()
            self.reallistvar = self.reallistvar()
            self.statevar = self.statevar()
            self.formvar = self.formvar()
            self.text = self.text()
            
    def _compart_names(self):
        """Get names of compartments
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        comparts = fnmatch.filter(node_names,'C*')
        return comparts
        
    def progvar(self):
        """Program variables
        """
        A = brace(1, 'A', 'Cross sectional area', 'm^2', 'A')
        P = brace(1, 'P', 'Perimeter length', 'm', 'P')
        Q = brace(1, 'Q', 'Discharge', 'm^3/h', 'Q')
        Sf = brace(1, 'Sf', 'Fiction slope', '', 'Sf')
        t = brace(1, 't', 'Time', 'h', 'T')
        w = brace(1, 'w', 'Surface width', 'm', 'W')
        x = brace(1, 'x', 'Space coordinate along the river', 'm', 'X')
        z0 = brace(1, 'z0', 'Water level elevation', 'm', 'Z0')
        return output('PROGVAR', A, P, Q, Sf, t, w, x, z0)
            
    def constvar(self):
        """Constant variables
        
            alpha: Angle river bed
            Qinit_comp: Initial discharge (for every compartment)
            sph: seconds per hour (conversion factor)
            hpd: hours per day (conversion factor)
            ugpg: ug per g (conversion factor)
            k_bio: rate of biodegradation
            hinit_comp: Initial water depth(for every compartment)
            Lc_comp: Compartment length (for every compartment)
            S_comp: Slope of river bed (for every compartment)
            yintercept_: y intersept riverbed elevation caluclation with slope
        """
        alpha = brace(1, 'alpha', 'Angle river bed', 'rad', 0, 1, 0, math.pi/2, 'TRUE', 'FALSE')
        sph = brace(1, 'sph', 'convert time from 1/s to 1/h', 's/h', 3600, 0.001, 0, 1000000, 'FALSE','FALSE')
        hpd = brace(1, 'hpd', 'convert time from d to h', 's/h', 24, 0.001, 0, 1000000, 'FALSE','FALSE')
        ugpg = brace(1, 'ugpg', 'convert wheight from ug to g', 'ug/g', 1000000, 0.001, 0, 10000000000, 'FALSE','FALSE')
        k_bio = brace(1, 'k_bio', 'rate of biodegradation', '1/h', self.k_bio, 0.001, 0, 10, 'FALSE', 'FALSE')
        Qinit_ = []
        hinit_ = []
        Lc_ = []
        S_ = []
        yintercept_ = []
        for compart in self._compart_names:
            node = self.hdf_input.get_node('/','{}/initial_conditions'.format(compart)).read()
            Qinit_name = 'Qinit_{}'.format(compart)
            Qinit_description = 'Initial discharge'
            Qinit_unit = 'm^3/s'
            Qinit_value = node['MQ'][0]
            Qinit_stdev = 1
            Qinit_min = 0
            Qinit_max = 2500
            Qinit_active_sa = 'FALSE'
            Qinit_active_pe = 'FALSE'
            Qinit_compart = brace(1, Qinit_name, Qinit_description, Qinit_unit, Qinit_value, Qinit_stdev, \
                                  Qinit_min, Qinit_max, Qinit_active_sa, Qinit_active_pe)
            Qinit_.append(Qinit_compart)
            hinit_name = 'hinit_{}'.format(compart)
            hinit_description = 'Initial water depth'
            hinit_unit = 'm'
            hinit_value = node['h'][0]
            hinit_stdev = 1
            hinit_min = 0
            hinit_max = 30
            hinit_active_sa = 'FALSE'
            hinit_active_pe = 'FALSE'
            hinit_compart = brace(1, hinit_name, hinit_description, hinit_unit, hinit_value, hinit_stdev, \
                                  hinit_min, hinit_max, hinit_active_sa, hinit_active_pe)
            hinit_.append(hinit_compart)
            Lc_name = 'Lc_{}'.format(compart)
            Lc_description = 'Compartment length'
            Lc_unit = 'm'
            Lc_value = node['comp_length'][0]
            Lc_stdev = 1
            Lc_min = 0
            Lc_max = 10000000
            Lc_active_sa = 'FALSE'
            Lc_active_pe = 'FALSE'
            Lc_compart = brace(1, Lc_name, Lc_description, Lc_unit, Lc_value, Lc_stdev, \
                                  Lc_min, Lc_max, Lc_active_sa, Lc_active_pe)
            Lc_.append(Lc_compart)
            S_name = 'S_{}'.format(compart)
            S_description = 'Slope of river bed in compartment {}'.format(compart)
            S_unit = ''
            S_value = (node['zb_end'][0] - node['zb_0'][0]) / node['comp_length'][0]
            S_stdev = 0.0001
            S_min = 0.00001
            S_max = 0.1
            S_active_sa = 'FALSE'
            S_active_pe = 'FALSE'
            S_compart = brace(1, S_name, S_description, S_unit, S_value, S_stdev, \
                                  S_min, S_max, S_active_sa, S_active_pe)
            S_.append(S_compart)
            yintercept_name = 'yintercept_{}'.format(compart)
            yintercept_description = 'Y-intercept for river bed elevation calculation with slope'
            yintercept_unit = 'm'
            yintercept_value = node['zb_0'][0] - S_value * node['start_x'][0]
            yintercept_stdev = 10
            yintercept_min = 1
            yintercept_max = 1000
            yintercept_active_sa = 'FALSE'
            yintercept_active_pe = 'FALSE'
            yintercept_compart = brace(1, yintercept_name, yintercept_description, yintercept_unit, yintercept_value, yintercept_stdev, \
                                  yintercept_min, yintercept_max, yintercept_active_sa, yintercept_active_pe)
            yintercept_.append(yintercept_compart)
        return output('CONSTVAR', alpha, sph, hpd, ugpg, k_bio, Qinit_, hinit_, Lc_, S_, yintercept_)
        
    def reallistvar(self):
        """List variables: Time-series or spatial data
        
            w_comp: River bed width (for every compartment)
            zB_comp: River bed elevation (for every compartment) // alternative option often causing problems in Aquasim 
            Kst_comp: Strickler coefficient (for every compartment)
            Qin_comp: Upstream input discharge (for every compartment)
            Qlat_comp: Lateral input discharge (for every compartment)
            Min_comp: Upstream input of substance, upstream load (for every compartment)
            Mlat_comp: Lateral input of substance, lateral load (for every compartment)
        """
        w_ = []
        zB_ = []
        Kst_ = []
        Qin_ = []
        Qlat_ = []
        Min_ = []
        Mlat_ = []
        for compart in self._compart_names:
            node = self.hdf_input.get_node('/','{}/parameterization'.format(compart))
            w_name = 'w_'+compart
            w_description = 'River bed width'
            w_unit = 'm'
            w_arg = 'x'
            w_stdev = 'TRUE'
            w_relstdev = 0
            w_absstdev = 1
            w_min = 0
            w_max =4000
            w_interpol = 'LINEAR'
            w_smoothw = 1
            w_active_sa = 'FALSE'
            w_stdev_list = 'FALSE'
            w_values = zipping(node, 'x', 'width')
            w_compart = brace(1, w_name, w_description, w_unit, w_arg, w_stdev, w_relstdev, w_absstdev, \
                              w_min, w_max, w_interpol, w_smoothw, w_active_sa, w_stdev_list, brace(w_values))
            w_.append(w_compart)
            zB_name ='zB_'+compart
            zB_description = 'River bed elevation'
            zB_unit = 'm'
            zB_arg = 'x'
            zB_stdev = 'TRUE'
            zB_relstdev = 0
            zB_absstdev = 1
            zB_min = -10
            zB_max = 4000
            zB_interpol = 'LINEAR'
            zB_smoothw = 1
            zB_active_sa = 'FALSE'
            zB_stdev_list = 'FALSE'
            zB_values = zipping(node, 'x', 'zb')
            zB_compart = brace(1, zB_name, zB_description, zB_unit, zB_arg, zB_stdev, zB_relstdev, zB_absstdev, \
                              zB_min, zB_max, zB_interpol, zB_smoothw, zB_active_sa, zB_stdev_list, brace(zB_values))
            zB_.append(zB_compart)
            Kst_name = 'Kst_'+compart
            Kst_description = 'Strickler coefficient'
            Kst_unit = 'm^(1/3)/s'
            Kst_arg = 'x'
            Kst_stdev = 'TRUE'
            Kst_relstdev = 0
            Kst_absstdev = 1
            Kst_min = 0
            Kst_max = 100
            Kst_interpol = 'LINEAR'
            Kst_smoothw = 1
            Kst_active_sa = 'TRUE' 
            Kst_stdev_list = 'FALSE'
            Kst_values = zipping(node, 'x', 'Kst')
            Kst_compart = brace(1, Kst_name, Kst_description, Kst_unit, Kst_arg, Kst_stdev, Kst_relstdev, Kst_absstdev, \
                               Kst_min, Kst_max, Kst_interpol, Kst_smoothw, Kst_active_sa, Kst_stdev_list, brace(Kst_values))
            Kst_.append(Kst_compart)
            node = self.hdf_input.get_node('/','{}/upstream_input'.format(compart))
            Qin_name = 'Qin_'+compart
            Qin_description = 'Upstream input discharge'
            Qin_unit = 'm^3/s'
            Qin_arg = 't'
            Qin_stdev = 'TRUE'
            Qin_relstdev = 0
            Qin_absstdev = 1
            Qin_min = 0
            Qin_max = 10000
            Qin_interpol = 'LINEAR'
            Qin_smoothw = 1
            Qin_active_sa = 'FALSE' 
            Qin_stdev_list = 'FALSE'
            Qin_values = zipping(node, 't', 'discharge')
            Qin_compart = brace(1, Qin_name, Qin_description, Qin_unit, Qin_arg, Qin_stdev, Qin_relstdev, Qin_absstdev, \
                               Qin_min, Qin_max, Qin_interpol, Qin_smoothw, Qin_active_sa, Qin_stdev_list, brace(Qin_values))
            Qin_.append(Qin_compart)
            node = self.hdf_input.get_node('/','{}/lateral_input'.format(compart))
            Qlat_name = 'Qlat_'+compart
            Qlat_description = 'Lateral input discharge'
            Qlat_unit = 'm^3/s'
            Qlat_arg = 't'
            Qlat_stdev = 'TRUE'
            Qlat_relstdev = 0
            Qlat_absstdev = 1
            Qlat_min = 0
            Qlat_max = 10000
            Qlat_interpol = 'LINEAR'
            Qlat_smoothw = 1
            Qlat_active_sa = 'FALSE' 
            Qlat_stdev_list = 'FALSE'
            Qlat_values = zipping(node, 't', 'discharge')
            Qlat_compart = brace(1, Qlat_name, Qlat_description, Qlat_unit, Qlat_arg, Qlat_stdev, Qlat_relstdev, Qlat_absstdev, \
                               Qlat_min, Qlat_max, Qlat_interpol, Qlat_smoothw, Qlat_active_sa, Qlat_stdev_list, brace(Qlat_values))
            Qlat_.append(Qlat_compart)
            node = self.hdf_input.get_node('/','{}/upstream_input'.format(compart))
            Min_name = 'Min_'+compart
            Min_description = 'Upstream input load'
            Min_unit = 'g/h'
            Min_arg = 't'
            Min_stdev = 'TRUE'
            Min_relstdev = 0
            Min_absstdev = 1
            Min_min = 0
            Min_max = 10000
            Min_interpol = 'LINEAR'
            Min_smoothw = 1
            Min_active_sa = 'FALSE' 
            Min_stdev_list = 'FALSE'
            Min_values = zipping(node, 't', 'load_aggregated')
            Min_compart = brace(1, Min_name, Min_description, Min_unit, Min_arg, Min_stdev, Min_relstdev, Min_absstdev, \
                               Min_min, Min_max, Min_interpol, Min_smoothw, Min_active_sa, Min_stdev_list, brace(Min_values))
            Min_.append(Min_compart)
            node = self.hdf_input.get_node('/','{}/lateral_input'.format(compart))
            Mlat_name = 'Mlat_'+compart
            Mlat_description = 'Upstream input load'
            Mlat_unit = 'g/h'
            Mlat_arg = 't'
            Mlat_stdev = 'TRUE'
            Mlat_relstdev = 0
            Mlat_absstdev = 1
            Mlat_min = 0
            Mlat_max = 10000
            Mlat_interpol = 'LINEAR'
            Mlat_smoothw = 1
            Mlat_active_sa = 'FALSE' 
            Mlat_stdev_list = 'FALSE'
            Mlat_values = zipping(node, 't', 'load_aggregated')
            Mlat_compart = brace(1, Mlat_name, Mlat_description, Mlat_unit, Mlat_arg, Mlat_stdev, Mlat_relstdev, Mlat_absstdev, \
                               Mlat_min, Mlat_max, Mlat_interpol, Mlat_smoothw, Mlat_active_sa, Mlat_stdev_list, brace(Mlat_values))
            Mlat_.append(Mlat_compart)
        return output('REALLISTVAR', w_, Kst_, Qin_, Qlat_, Min_, Mlat_, zB_)
    
    def statevar(self):
        """State variables
        
            C: Concentration
        """
        unknown = 1
        name = "C"
        description = "Concentration"
        unit = "ug/m3"
        var = "VOL"
        relacc = 1e-006
        absacc = 1e-006
        C = brace(unknown, name, description, unit, var, relacc, absacc)
        return output("STATEVAR", C)
        
    def formvar(self):
        """ Formula variable
        
            d: Mean river depth
            v: Velocity
            h_compart: Maximum water depth (for every compartment)
            A_compart: Cross secitonal area (for every compartment)
            P_compart: Cross sectional perimeter (for every compartment)
            z0init_compart: Initial water level zB+hinit (for every compartment)
            zB_compart: River bed elevation (for every compartment)
        """
        d = brace(1, 'd', 'Mean river depth', 'm', 'A/w')
        v = brace(1, 'v', 'Velocity', 'm/h', 'Q/A')
        Qplot = brace(1, 'Qplot', 'Discharge for plotting', 'm^3/s', 'Q/sph')
        h_ = []
        A_ = []
        P_ = []
        z0init_ = []
#        zB_ =[]
        for compart in self._compart_names:
#            zB_name = 'zB_'+compart
#            zB_description = 'River bed elevation'
#            zB_unit = 'm'
#            zB_expression = 'S_{0}*x+yintercept_{0}'.format(compart)
#            zB_compart = brace(1, zB_name, zB_description, zB_unit, zB_expression)
#            zB_.append(zB_compart) 
            h_name = 'h_'+compart
            h_description = 'Maximum water depth'
            h_unit = 'm'
            h_expression = 'z0-zB_'+compart
            h_compart = brace(1, h_name, h_description, h_unit, h_expression)
            h_.append(h_compart)
            A_name = 'A_'+compart
            A_description = 'Cross sectional area in compartment '+compart
            A_unit = 'm^2'
            A_expression = 'h_{0}*(w_{0}+h_{0}*(sin(alpha)/cos(alpha)))'.format(compart)
            A_compart = brace(1, A_name, A_description, A_unit, A_expression)
            A_.append(A_compart)
            P_name = 'P_'+compart
            P_description = 'Cross sectional perimeter of compartment '+compart
            P_unit = 'm'
            P_expression = 'w_{0}+2*h_{0}/cos(alpha)'.format(compart)         
            P_compart = brace(1, P_name, P_description, P_unit, P_expression)
            P_.append(P_compart)
            z0init_name = 'z0init_'+compart
            z0init_description = 'Initial water level'
            z0init_unit = 'm'
            z0init_expression = 'zB_{0}+hinit_{0}'.format(compart)
            z0init_compart = brace(1, z0init_name, z0init_description, z0init_unit, z0init_expression)
            z0init_.append(z0init_compart)
        return output('FORMVAR', d, v, Qplot, h_, A_, P_, z0init_ ) # , zB_)
                        
    def text(self):
        """Run thread.
        """
        return self.progvar+self.constvar+self.reallistvar+self.formvar
        
        
class ProcSys(object):
    """Process System
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.dynproc = self.dynproc()
            
    def dynproc(self):
        """Dynamic processes
        
            biodegradation: degradation in water
        """
        unknown = 1
        name = 'biodegradation'
        description = 'degradation process in water'
        rate = 'k_bio*C'
        stoichiometry = brace('C', -1)
        biodegradation = brace(unknown, name, description, rate, stoichiometry)
        return output('DYNPROC', biodegradation)
        

class CompSys(object):
    """Compartment System
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['output_aqu_compartment_path']
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            self._compart_names = self._compart_names()
            self._last_compart = self._last_compart()
            self.rivcomp = self.rivcomp()
            
    def _compart_names(self):
        """Return list of compartment names        
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        comparts = fnmatch.filter(node_names,'C*')
        return comparts
    
    def _get_start_coord(self, compart):
        """Return start coordinate of compartment
        """
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            node = self.hdf_input.get_node('/','{}/initial_conditions'.format(compart))
            table = node.read()
            x0 = table['start_x'][0]
        return x0
        
    def _get_end_coord(self, compart):
        """Return en coordinate of compartment
        """
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            node = self.hdf_input.get_node('/','{}/initial_conditions'.format(compart))
            table = node.read()
            x0 = table['start_x']
            L = table['comp_length']
        return x0+L
    
    def _last_compart(self):
        """Return last compartment of riversystem.
        """
        node = self.hdf_input.get_node('/','links')
        table = node.read()
        compart = table['fromCompart']
        res = [i for i in self._compart_names if i.encode('utf-8') not in compart]
        return res
            
    def rivcomp(self):
        """River compartments
            
            unknown: must be 6 (unclear what this value means - 5 or 6 in the examples)
            name: Name of compartment
            description: Description of compartment
            comp_index: Compartment index to make process rates dependent on the compartment (>0)
            variables: List of active variables {var1}{var2}...
            processes: List of active processes {proc1}{proc2}...
            active_calc: Active/inactive for calculations 'TRUE'/'FALSE'
            up_input: Upstream water inflow
            up_var: Upstream loads {conc_var1}{up_load1}{conc_var2}{up_load2}...
            init_cond: List of initial conditions {var1}{cond1}{var2}{cond2}
            lat_input: Lateral water inflow
            lat_var: Lateral concentrations {conc_var1}{lat_conc1}{conc_var2}{lat_conc2}...
            grid_pts: number of grid points for discretization
            resolution: 'FALSE' for low resolution and 'TRUE' for high resolution
            Q_relacc: relative numerical accuracy of program variable discharge
            Q_absacc: absolute numerical accuracy of program variable discharge
            A_relacc: relative numerical accuracy of program variable cross section
            A_absacc: absolute numerical accuracy of program variable cross section
            z0_relacc: relative numerical accuracy of program variable water level
            z0_absacc: absolute numerical accuracy of program variable water level
            D_relacc: relative numerical accuracy of program variable dispersion
            D_absacc: absolute numerical accuracy of program variable dispersion
            start_coord: start coordinate x (length)
            end_coord: end coordinate x (length)
            grav_accel: gravitational acceleration (9.81 m/s^2 * 3600^2 s^2/h^2 = 1.27e+008 m/h^2)
            cross_sec: cross section of compartment
            perimeter: perimeter section of compartment
            width: widt section of compartment
            fric_slope:
            dispersion: 'TRUE' with dispersion, 'FALSE' without dispersion
            dispersion_eq: dispersion equation
            sediment_mode: 'TRUE' with sediments, 'FALSE' without sediments
            sediment_prop: sediment properties
            end_level_given: end level if 'diffusive' approach and 'given' is selcted
            end_level = 'NORMAL', 'CRITICAL', 'GIVEN'
            method = 'KIN' for kinematic, 'DIFF' for diffusive
        """
        compartments = []
        for compart in self._compart_names:
            unknown = 6 
            name = compart
            description = 'River reach '+compart
            comp_index = 0
            variables = brace('C')
            processes = brace('biodegradation')
            active_calc = 'TRUE'
            up_input = 'Qin_{}*sph'.format(compart)
            up_var = brace('C', 'Min_{}*ugpg'.format(compart))
            init_cond = brace(0, 'Q', 'Qinit_{}*sph'.format(compart), 0, 'z0', 'z0init_{}'.format(compart))
            lat_input = 'Qlat_{0}/Lc_{0}*sph'.format(compart)
            lat_var = brace('C', '(Mlat_{0}*ugpg)/(Qlat_{0}*sph)'.format(compart))
            grid_pts = 8
            resolution = 'FALSE'
            Q_relacc = 0.001
            Q_absacc = 0.001
            A_relacc = 0.001
            A_absacc = 0.001
            z0_relacc = 0
            z0_absacc = 1e-006
            D_relacc = 1e-006
            D_absacc = 1e-006
            start_coord = self._get_start_coord(compart)
            end_coord = self._get_end_coord(compart)
            grav_accel = 1.27e+008 #m/h2
            cross_sec = 'A_'+compart
            perimeter = 'P_'+compart
            width = 'w_'+compart
            fric_slope = '1/(Kst_{}*sph)^2*(P/A)^(4/3)*(v)^2'.format(compart)
            dispersion = 'FALSE'
            dispersion_eq = ''
            sediment_mode = 'FALSE'
            sediment_prop = ''
            end_level_given = ''
            end_level = 'NORMAL'
            method = 'KIN'
            if compart in self._last_compart:
                end_level_given = 4                                             ### change to initial water depth
                end_level = 'GIVEN'
                method = 'DIFF'
            compartment = brace(unknown, name, description, comp_index, variables, processes, active_calc, \
                                 up_input, up_var, init_cond, lat_input, lat_var, grid_pts, resolution, Q_relacc, \
                                 Q_absacc,A_relacc, A_absacc, z0_relacc, z0_absacc, D_relacc, D_absacc, start_coord, \
                                 end_coord, grav_accel, cross_sec, perimeter, width, fric_slope, dispersion, \
                                 dispersion_eq, sediment_mode, sediment_prop, end_level_given, end_level, method)
            compartments.append(compartment)
        return output('RIVCOMP', compartments)
            
            
            
class LinkSys(object):
    """Link System
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['output_aqu_compartment_path']
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            self._links = self._links()
            self.advlink = self.advlink()
    
    def _links(self):
        """Read node 'links' and return table with columns 'fromCompart' and 'toCompart'
        """   
        node = self.hdf_input.get_node('/','links')
        table = node.read()
        return table
            
    def advlink(self):
        """Advective Links
        
            unknown: 
            name: Name of link
            description: Description of link
            index: Index to make process rates dependent on the link (>0)
            from_compart: Name of compartment that feed into the link
            connection1: Output connection (for compartments with bifurcations) 
            to_compart: Name of compartment
            connection2: Input connection (for compartments with bifurcations) 
            bifurcation: Name, description, toCompart, water flow and Loadings for bifurcations             
        """
        advlinks = []
        for link in self._links:
            unknown = 2
            name = link['Name'].decode('ascii')
            description = ''
            index = 0
            from_compart = link['fromCompart'].decode('ascii')
            connection1 = 0
            to_compart = link['toCompart'].decode('ascii')
            connection2 = 0
            bifurcation = ''
            advlink = brace(unknown, name, description, index, from_compart, connection1, to_compart, \
                           connection2, bifurcation)
            advlinks.append(advlink)
        return output('ADVLINK', advlinks)
            
        
class CalcSys(object):
    """Calculation System
    """
    def __init__(self, config_file):
        config = read_config(config_file)
        self.input_file_name = config['routing_model']['output_aqu_compartment_path']
        with tables.open_file(self.input_file_name, mode='r') as self.hdf_input:
            self._compart_names = self._compart_names()
            self._timesteps = self._timesteps()
            self.calc = self.calc()
            
    def _compart_names(self):
        """Return list of compartment names        
        """
        node = self.hdf_input.get_node('/')
        node_names = [i._v_name for i in node._f_list_nodes()]
        comparts = fnmatch.filter(node_names,'C*')
        return comparts
            
    def _timesteps(self):
        """Return number of timesteps in arbitrary compartemnt upstream data
        """
        compart1 = self._compart_names[1]
        node = self.hdf_input.get_node('/', '{}/upstream_input'.format(compart1))
        table = node.read()
        return len(table['t'])
    
    def calc(self):
        """Calculation Definition            
        
            unknown: 
            name: Name of calculation
            description: Description of Calculation Definition
            calc_number: 
            initial_time: 
            initial_state: 'FALSE' for "given, made consistent", 'TRUE' for "steady state"
            active_simulation: Active for simulation
            active_sa: Active for sensitivity analysis
        """
        steps = [10, self._timesteps] # 99 timesteps or as many as in the HDF5 table
        active = ['FALSE','TRUE']
        calcs = []
        for i in range(0,len(steps)):
            unknown = 2
            name = 'calc{}'.format(steps[i])
            description = ''
            calc_number = 0
            initial_time = 0
            initial_state = 'FALSE'
            output_steps = brace(1, steps[i])
            active_sim = active[i]
            active_sa = 'FALSE'
            calc = brace(unknown, name, description, calc_number, initial_time, initial_state, output_steps, \
                        active_sim, active_sa)
            calcs.append(calc)
        return output('CALC', calcs)