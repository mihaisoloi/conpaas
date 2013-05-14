# -*- coding: utf-8 -*-

"""
    cpsdirector.service
    ===================

    ConPaaS director: services implementation

    :copyright: (C) 2013 by Contrail Consortium.
"""

from flask import Blueprint
from flask import jsonify, helpers, request, make_response, g

from sqlalchemy.exc import InvalidRequestError

import sys
import traceback

import simplejson
from datetime import datetime

from cpsdirector import db

from cpsdirector.common import log
from cpsdirector.common import build_response

from cpsdirector import cloud as manager_controller

from cpsdirector import common

from conpaas.core.services import manager_services
from conpaas.core.https.client import jsonrpc_post, check_response

service_page = Blueprint('service_page', __name__)

# Manually add task farming to the list of valid services
valid_services = manager_services.keys() + ['taskfarm', ]

class Service(db.Model):
    sid = db.Column(db.Integer, primary_key=True,
        autoincrement=True)
    name = db.Column(db.String(256))
    type = db.Column(db.String(32))
    state = db.Column(db.String(32))
    created = db.Column(db.DateTime)
    manager = db.Column(db.String(512))
    vmid = db.Column(db.String(256))
    cloud = db.Column(db.String(128))
    subnet = db.Column(db.String(18))

    user_id = db.Column(db.Integer, db.ForeignKey('user.uid'))
    user = db.relationship('User', backref=db.backref('services',
        lazy="dynamic"))

    application_id = db.Column(db.Integer, db.ForeignKey('application.aid'))
    application = db.relationship('Application', backref=db.backref('services',
                                  lazy="dynamic"))

    def __init__(self, **kwargs):
        # Default values
        self.state = "INIT"
        self.created = datetime.now()

        for key, val in kwargs.items():
            setattr(self, key, val)

    def to_dict(self):
        ret = {}
        for c in self.__table__.columns:
            ret[c.name] = getattr(self, c.name)
            if type(ret[c.name]) is datetime:
                ret[c.name] = ret[c.name].isoformat()

        return ret

    def stop(self):
        controller = manager_controller.ManagerController(self.type,
                self.sid, self.user_id, self.cloud, self.application_id,
                self.subnet)

        controller.stop(self.vmid)
        db.session.delete(self)
        db.session.commit()
        log('Service %s stopped properly' % self.sid)
        return True

def get_service(user_id, service_id):
    service = Service.query.filter_by(sid=service_id).first()
    if not service:
        log('Service %s does not exist' % service_id)
        return

    if service.user_id != user_id:
        log('Service %s is not owned by user %s' % (service_id, user_id))
        return

    return service

@service_page.route("/available_services", methods=['GET'])
def available_services():
    """GET /available_services"""
    return simplejson.dumps(valid_services)

from cpsdirector.application import get_default_app, get_app

from cpsdirector.user import cert_required

@service_page.route("/start/<servicetype>", methods=['POST'])
@service_page.route("/start/<servicetype>/<cloudname>", methods=['POST'])
@cert_required(role='user')
def start(servicetype, cloudname="default"):
    """eg: POST /start/php

    POSTed values might contain 'appid' to specify that the service to be
    created has to belong to a specific application. If 'appid' is omitted, the
    service will belong to the default application.

    Returns a dictionary with service data (manager's vmid and IP address,
    service name and ID) in case of successful authentication and correct
    service creation. False is returned otherwise.
    """
    appid = request.values.get('appid')

    # Use default application id if no appid was specified
    if not appid:
        appid = get_default_app(g.user.uid).aid

    log('User %s attempting creation of new %s service inside application %s'
        % (g.user.username, servicetype, appid))


    def return_error(msg):
        log(msg)
        return build_response(jsonify({ 'error': True,
                                        'msg': msg }))


    # Check if we got a valid service type
    if servicetype not in valid_services:
        return_error('Unknown service type: %s' % servicetype)

    ft = get_faulttolerance(cloudname)
    #check to see if there already is a ft service that runs on the cloud
    if servicetype == 'faulttolerance' and ft:
        return_error('FaultTolerance already running on cloud %s at %s'
                     % (ft[0].cloud, ft[0].manager))

    app = get_app(g.user.uid, appid)
    if not app:
        return_error("Application not found" )

    # Do we have to assign a VPN subnet to this service?
    vpn = app.get_available_vpn_subnet()

    # New service with default name, proper servicetype and user relationship
    s = Service(name="New %s service" % servicetype, type=servicetype,
        user=g.user, application=app, subnet=vpn)

    db.session.add(s)
    # flush() is needed to get auto-incremented sid
    db.session.flush()

    try:
        s.manager, s.vmid, s.cloud = manager_controller.start(
            servicetype, s.sid, g.user.uid, cloudname, appid, vpn)
    except Exception, err:
        try:
            db.session.delete(s)
            db.session.commit()
        except InvalidRequestError:
            db.session.rollback()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        log(''.join('!! ' + line for line in lines))
        error_msg = 'Error upon service creation: %s %s' % (type(err), err)
        log(error_msg)
        return build_response(jsonify({ 'error': True, 'msg': error_msg }))

    db.session.commit()

    if ft: #only one ft manager per cloud
       check_response(jsonrpc_post(ft[0].manager, 443, '/', 'register',
                                   params = {'datasources':
                                   __all_services_to_datasources(cloudname)}))

    log('%s (id=%s) created properly' % (s.name, s.sid))
    return build_response(jsonify(s.to_dict()))


def __all_services_to_datasources(cloudname):
    '''
        Ganglia metad needs all the services to watch so we have to
        pass again all of the services each time we register a new one
    '''
    from conpaas.core.ganglia import Datasource
    if cloudname == "default":
        cloudname = "iaas"
    return [Datasource('%s-u%s-s%s' % (s.type, s.user_id, s.sid), s.manager)
            for s in Service.query.filter_by(cloud = cloudname,
                                             user_id = g.user.uid)
            if s.type != "faulttolerance"]


def get_faulttolerance(cloudname="default"):
    '''
       Gets the details of the faulttolerance service on the specific cloud
    '''
    if cloudname == "default":
        cloudname = "iaas"
    return [s for s in Service.query.filter_by(type = "faulttolerance",
                                               cloud = cloudname,
                                               user_id = g.user.uid)]


@service_page.route("/rename/<int:serviceid>", methods=['POST'])
@cert_required(role='user')
def rename(serviceid):
    log('User %s attempting to rename service %s' % (g.user.uid, serviceid))

    service = get_service(g.user.uid, serviceid)
    if not service:
        return make_response(simplejson.dumps(False))

    newname = request.values.get('name')
    if not newname:
        log('"name" is a required argument')
        return build_response(simplejson.dumps(False))

    service.name = newname
    db.session.commit()
    return simplejson.dumps(True)

@service_page.route("/callback/terminateService.php", methods=['POST'])
@cert_required(role='manager')
def terminate():
    """Terminate the service whose id matches the one provided in the manager
    certificate."""
    log('User %s attempting to terminate service %s' % (g.user.uid,
                                                        g.service.sid))

    if g.service.stop():
        return jsonify({ 'error': False })

    return jsonify({ 'error': True })

@service_page.route("/stop/<int:serviceid>", methods=['POST'])
@cert_required(role='user')
def stop(serviceid):
    """eg: POST /stop/3

    POSTed values must contain username and password.

    Returns a boolean value. True in case of successful authentication and
    proper service termination. False otherwise.
    """
    log('User %s attempting to stop service %s' % (g.user.uid, serviceid))

    service = get_service(g.user.uid, serviceid)
    if not service:
        return build_response(simplejson.dumps(False))

    # If a service with id 'serviceid' exists and user is the owner
    service.stop()
    return build_response(simplejson.dumps(True))


@service_page.route("/list", methods=['POST', 'GET'])
@cert_required(role='user')
def list_all_services():
    """POST /list

    List running ConPaaS services under a specific application if the user is
    authenticated. Return False otherwise.
    """
    return build_response(simplejson.dumps([
        ser.to_dict() for ser in g.user.services.all()
    ]))

@service_page.route("/list/<int:appid>", methods=['POST', 'GET'])
@cert_required(role='user')
def list_services(appid):
    """POST /list/2

    List running ConPaaS services under a specific application if the user is
    authenticated. Return False otherwise.
    """
    return build_response(simplejson.dumps([
        ser.to_dict() for ser in Service.query.filter_by(application_id=appid)
    ]))

@service_page.route("/download/ConPaaS.tar.gz", methods=['GET'])
def download():
    """GET /download/ConPaaS.tar.gz

    Returns ConPaaS tarball.
    """
    return helpers.send_from_directory(common.config_parser.get('conpaas', 'CONF_DIR'),
        "ConPaaS.tar.gz")
