/**
 * Put your copyright and license info here.
 */
package com.datatorrent.hdhtprob;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randGen = dag.addOperator("Rand Gen", RandomNumberGenerator.class);
    HDHTTestOperator hdhtOperator = dag.addOperator("HDHT Operator", HDHTTestOperator.class);

    TFileImpl storeFile = new TFileImpl.DTFileImpl();
    storeFile.setBasePath("hdhtprobtest");

    hdhtOperator.setFileStore(storeFile);

    dag.addStream("HDHTStream", randGen.out, hdhtOperator.input);
  }
}
