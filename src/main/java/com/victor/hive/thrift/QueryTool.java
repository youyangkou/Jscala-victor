package com.victor.hive.thrift;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

/**
 * @author Gerry
 * @date 2022-08-02
 */
public class QueryTool {

    public static TTransport getSocketInstance(String host, int port, String USER, String PASSWORD) throws TTransportException {

        TTransport transport = HiveAuthFactory.getSocketTransport(host, port, 99999);
        try {
//            transport = PlainSaslHelper.getPlainTransport(USER, PASSWORD, transport);
            TTransportFactory plainTransportFactory = PlainSaslHelper.getPlainTransportFactory(HiveAuthFactory.AuthTypes.NOSASL.toString());
            transport = plainTransportFactory.getTransport(PlainSaslHelper.getPlainTransport(USER, PASSWORD, transport));

        } catch (SaslException | LoginException e) {
            e.printStackTrace();
        }
        return transport;
    }

    public static TOpenSessionResp openSession(TCLIService.Client client) throws TException {
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        return client.OpenSession(openSessionReq);
    }
}

