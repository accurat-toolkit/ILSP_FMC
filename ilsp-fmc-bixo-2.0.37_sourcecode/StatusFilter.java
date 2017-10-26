package gr.ilsp.fmc.operations;


import gr.ilsp.fmc.datums.StatusOutputDatum;

import java.io.Serializable;



import bixo.datum.StatusDatum;



@SuppressWarnings("serial")
public class StatusFilter implements Serializable {

	public StatusOutputDatum filter(StatusDatum statusDatum) {
		StatusOutputDatum datum = new StatusOutputDatum();
		//datum.setTuple(statusDatum.getTuple());
		datum.setPayload(statusDatum.getPayload());		
		String exception = "";
		if (statusDatum.getException()!=null) exception = statusDatum.getException().getMessage(); 
		datum.setException(exception);
		datum.setHeaders(statusDatum.getHeaders().toString());
		datum.setHostAddress(statusDatum.getHostAddress());
		datum.setStatus(statusDatum.getStatus());
		datum.setStatusTime(statusDatum.getStatusTime());
		datum.setUrl(statusDatum.getUrl());
		
		
		//datum.setUrl(statusDatum.getUrl());		
		// TODO Auto-generated method stub
		return datum;
	}
	
}