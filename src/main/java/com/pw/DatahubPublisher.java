package com.pw;

import io.datahubproject.openapi.generated.*;
import datahub.client.rest.RestEmitter;
import datahub.event.UpsertAspectRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DatahubPublisher {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        List<UpsertAspectRequest> requests = new ArrayList<>();
        RestEmitter emitter = RestEmitter.createWithDefaults();
        String inputDataset1Urn = "urn:li:dataset:(urn:li:dataPlatform:s3,in-dataset-1,PROD)";
        TimeStamp created = new TimeStamp().time(1675219945L);
        AuditStamp createdAuditStamp = new AuditStamp().actor("urn:li:corpuser:eyomi").time(created.getTime());
        AuditStamp lastModifiedAuditStamp = new AuditStamp().actor("urn:li:corpuser:eyomi").time(Instant.now().getEpochSecond());

        UpsertAspectRequest upsertInputDataset1 = UpsertAspectRequest.builder()
                .entityType("dataset").entityUrn(inputDataset1Urn)
                .aspect(new DatasetProperties().name("Input Dataset 1").created(created).description("Input Dataset 1"))
                .aspect(new DatasetProfile().columnCount(1L).timestampMillis(Instant.now().toEpochMilli()))
                .build();
        String inputDataset2Urn = "urn:li:dataset:(urn:li:dataPlatform:s3,in-dataset-2,PROD)";
        UpsertAspectRequest upsertInputDataset2 = UpsertAspectRequest.builder()
                .entityType("dataset").entityUrn(inputDataset2Urn)
                .aspect(new DatasetProperties().name("Input Dataset 2").created(created).description("Input Dataset 2"))
                .aspect(new DatasetProfile().columnCount(1L).timestampMillis(Instant.now().toEpochMilli()))
                .build();
        String outputDataset1Urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,out-dataset-1,PROD)";
        UpsertAspectRequest upsertOutputDataset1 = UpsertAspectRequest.builder()
                .entityType("dataset").entityUrn(outputDataset1Urn)
                .aspect(new DatasetProperties().name("Output Dataset 1").created(created).description("Output Dataset 1"))
                .aspect(new DatasetProfile().columnCount(1L).timestampMillis(Instant.now().toEpochMilli()))
                .build();
        String outputDataset2Urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,out-dataset-2,PROD)";
        UpsertAspectRequest upsertOutputDataset2 = UpsertAspectRequest.builder()
                .entityType("dataset").entityUrn(outputDataset2Urn)
                .aspect(new DatasetProperties().name("Output Dataset 2").created(created).description("Output Dataset 2"))
                .aspect(new DatasetProfile().columnCount(1L).timestampMillis(Instant.now().toEpochMilli()))
                .build();

        String ltmrfUrn = "urn:li:dataProduct:(urn:li:dataPlatform:f135DataMesh,ltmrf,PROD)";
        UpsertAspectRequest upsertDataProduct = UpsertAspectRequest.builder()
                .entityType("dataProduct")
                .entityUrn(ltmrfUrn)
                .aspect(new DataProductProperties()
                        .name("Long Term Module Removals Forecasts")
                        .created(created)
                        .description("This is a data product for long term module removals forecasts"))
                .aspect(new DataProductInputOutput()
                        .inputDatasetEdges(List.of(new Edge().sourceUrn(inputDataset1Urn).destinationUrn(ltmrfUrn).created(createdAuditStamp).lastModified(lastModifiedAuditStamp), new Edge().sourceUrn(inputDataset2Urn).destinationUrn(ltmrfUrn).created(createdAuditStamp).lastModified(lastModifiedAuditStamp)))
                        .outputDatasetEdges(List.of(new Edge().sourceUrn(ltmrfUrn).destinationUrn(outputDataset1Urn).created(createdAuditStamp).lastModified(lastModifiedAuditStamp), new Edge().sourceUrn(ltmrfUrn).destinationUrn(outputDataset2Urn).created(createdAuditStamp).lastModified(lastModifiedAuditStamp))))
                .build();

        requests.add(upsertInputDataset1);
        requests.add(upsertInputDataset2);
        requests.add(upsertOutputDataset1);
        requests.add(upsertOutputDataset2);
        requests.add(upsertDataProduct);

        System.out.println(emitter.emit(requests, null).get());
        System.exit(0);
    }
}
