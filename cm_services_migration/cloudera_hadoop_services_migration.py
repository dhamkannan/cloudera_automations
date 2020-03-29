from cm_api.api_client import ApiResource
import uuid
import time
import sys
from urllib2 import HTTPError


cm_host = raw_input("CM_Host:  ")
cm_username = raw_input("CM_Username:  ")
cm_password = getpass(prompt="CM_Password: ")
old_node = raw_input("CM_Host:  ")
new_node = raw_input("CM_Host:  ")

def migrate_services(cm_host, cm_username, cm_password, old_node, new_node):
    uid = str(uuid.uuid4().hex)
    api = ApiResource(cm_host, username=cm_username, password=cm_password)
    cluster = api.get_all_clusters()[0]
    migrate_hdfs(cluster, new_node, old_node, uid, api)
    migrate_hue(cluster, new_node, old_node, uid)
    migrate_impala(cluster, new_node, old_node, uid)
    migrate_spark(cluster, new_node, old_node, uid)
    migrate_spark2(cluster, new_node, old_node, uid)
    migrate_hive(cluster, new_node, old_node, uid)
    migrate_oozie(cluster, new_node, old_node, uid)
    migrate_zookeeper(cluster, new_node, old_node, uid)
    migrate_sentry(cluster, new_node, old_node, uid)
    migrate_solr(cluster, new_node, old_node, uid)
    migrate_yarn(cluster, new_node, old_node, uid)
    migrate_arcadia(cluster, new_node, old_node, uid)
    print ('Restarting cluster, please wait.....')
    time.sleep(30)
    cluster.restart().wait()
    print ('Migration of Roles are completed')


def migrate_role(role_type, service, existing_roles, new_node, uid, old_node, cluster):
    if ({role_type}.issubset(set(existing_roles.keys()))):
        print('Migrating role: ' + role_type)
        if role_type == 'GATEWAY':
            new_role_name = service.type.split('_')[0] + role_type + '_' + uid
        else:
            new_role_name = role_type.replace('_', '') + '_' + uid
        role_config = get_role_config(role_type, service, existing_roles)
        delete_role(existing_roles, role_type, service)
        create_role(new_node, new_role_name, role_type, service)
        update_role_config(role_config, service, new_role_name)
        if role_type == 'HTTPFS':
            update_hue_config_for_httpfs(cluster, new_role_name)
        deploy_client_config(cluster)
        start_role(new_role_name, role_type, service)
        print('Role migration is completed for role ' + role_type + '\n')


def start_role(new_role_name, role_type, service):
    if role_type != 'BALANCER':
        print('Starting new role ' + new_role_name)
        service.start_roles(new_role_name)
        time.sleep(30)


def deploy_client_config(cluster):
    print('Deploying client configurations')
    cluster.deploy_client_config()
    time.sleep(45)


def create_role(new_node, new_role_name, role_type, service):
    print('Created new role ' + new_role_name)
    service.create_role(new_role_name, role_type, new_node)
    time.sleep(30)


def delete_role(existing_roles, role_type, service):
    print('Deleting existing role ' + existing_roles[role_type])
    service.delete_role(existing_roles[role_type])
    time.sleep(30)


def update_role_config(role_config, service, new_role_name):
    for role in service.get_all_roles():
        if role.name == new_role_name:
            print('Updating role configurations for role ' + new_role_name)
            role.update_config(role_config)
    time.sleep(30)


def get_role_config(role_type, service, existing_roles):
    for role in service.get_all_roles():
        if role.name == existing_roles[role_type]:
            print('Getting existing role configurations for role ' + existing_roles[role_type])
            role_config = role.get_config()
            print('Existing Role config as below ')
            print (role_config)
    return role_config


def migrate_hdfs(cluster, new_node, old_node, uid, api):
    for service in cluster.get_all_services():
        if service.type == 'HDFS':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                migrate_FM_NN(api, existing_roles, new_node, old_node, service)
                migrate_JN('JOURNALNODE', api, existing_roles, new_node, old_node, service)
                migrate_role('BALANCER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('HTTPFS', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_JN(role_type, api, existing_roles, new_node, old_node, service):
    if ({role_type}.issubset(set(existing_roles.keys()))):
        print('Migrating Role - JOURNALNODE')
        role_config = get_role_config(role_type, service, existing_roles)
        print('Migration in Progress.... Please wait.....!!!')
        for host in api.get_all_hosts():
            if host.hostId == old_node:
                host.migrate_roles([existing_roles[role_type]], new_node, True).wait()
        new_role_name = get_role_name(service, new_node, role_type)
        update_role_config(role_config, service, new_role_name)
        print('Migrated Role - JOURNALNODE')


def migrate_FM_NN(api, existing_roles, new_node, old_node, service):
    if ({'FAILOVERCONTROLLER', 'NAMENODE'}.issubset(set(existing_roles.keys()))):
        print('Migrating Roles - HDFS FAILOVERCONTROLLER and HDFS NAMENODE')
        FM_config = get_role_config('FAILOVERCONTROLLER', service, existing_roles)
        NM_config = get_role_config('NAMENODE', service, existing_roles)
        for host in api.get_all_hosts():
            if host.hostId == old_node:
                host.migrate_roles(
                    [existing_roles['FAILOVERCONTROLLER'], existing_roles['NAMENODE']], new_node, True).wait()
        FM_role_name = get_role_name(service, new_node, 'FAILOVERCONTROLLER')
        update_role_config(FM_config, service, FM_role_name)
        NM_role_name = get_role_name(service, new_node, 'NAMENODE')
        update_role_config(NM_config, service, NM_role_name)
        print('Migrated Roles - HDFS FAILOVERCONTROLLER and HDFS NAMENODE')


def migrate_hue(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'HUE':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('KT_RENEWER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('HUE_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('HUE_LOAD_BALANCER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_yarn(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'YARN':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                migrate_RM('RESOURCEMANAGER', existing_roles, new_node, old_node, service)
                migrate_role('JOBHISTORY', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_RM(role_type, existing_roles, new_node, old_node, service):
    if ({role_type}.issubset(set(existing_roles.keys()))):
        print('Migrating role - YARN RESOURCEMANAGER')
        role_config = get_role_config(role_type, service, existing_roles)
        for role in service.get_all_roles():
            if role.type == role_type and role.hostRef.hostId != old_node:
                active_rm = role.name
        print('Disabling High availability on RESOURCEMANAGER on old node')
        service.disable_rm_ha(active_rm).wait()
        print('Enabling High availability on RESOURCEMANAGER on new node')
        service.enable_rm_ha(new_node).wait()
        new_role_name = get_role_name(service, new_node, role_type)
        time.sleep(30)
        update_role_config(role_config, service, new_role_name)
        print('Migrated role - YARN RESOURCEMANAGER')


def migrate_impala(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'IMPALA':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('STATESTORE', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('CATALOGSERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_spark2(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'SPARK2_ON_YARN':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('SPARK2_YARN_HISTORY_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_spark(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'SPARK_ON_YARN':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('SPARK_YARN_HISTORY_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_hive(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'HIVE':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('HIVESERVER2', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('HIVEMETASTORE', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('WEBHCAT', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_oozie(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'OOZIE':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('OOZIE_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_zookeeper(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'ZOOKEEPER':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_sentry(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'SENTRY':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('SENTRY_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_solr(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'SOLR':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('SOLR_SERVER', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('GATEWAY', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def migrate_arcadia(cluster, new_node, old_node, uid):
    for service in cluster.get_all_services():
        if service.type == 'ARCADIAENTERPRISE':
            existing_roles = get_roles(old_node, service)
            if existing_roles:
                stop_service(service)
                migrate_role('ARCVIZ', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('ARCENGINE_CATALOG', service, existing_roles, new_node, uid, old_node, cluster)
                migrate_role('ARCENGINE_STATESTORE', service, existing_roles, new_node, uid, old_node, cluster)
                start_service(service)
            else:
                print('No role present for the service:- ' + service.type)


def start_service(service):
    print('Starting service ' + service.name)
    service.start().wait()
    print('Started service ' + service.name + '\n')


def stop_service(service):
    print('Stopping service ' + service.name)
    service.stop().wait()
    print('Stopped service ' + service.name + '\n')


def get_roles(old_node, service):
    existing_roles = {}
    for role in service.get_all_roles():
        if role.hostRef.hostId == old_node:
            existing_roles[str(role.type)] = str(role.name)
    return existing_roles


def update_hue_config_for_httpfs(cluster, new_role_name):
    for service in cluster.get_all_services():
        if service.type == 'HUE':
            print('Updating new hue_webhdfs configuration on HUE')
            hue_config = service.get_config()
            hue_config[0][u'hue_webhdfs'] = new_role_name
            service.update_config(hue_config[0])
            print('Updated new hue_webhdfs configuration on HUE')


def get_role_name(service, new_node, role_type):
    for role in service.get_all_roles():
        if role.hostRef.hostId == new_node:
            if role.type == role_type:
                new_role_name = role.name
    return new_role_name

migrate_services(cm_host, cm_username, cm_password, old_node, new_node)