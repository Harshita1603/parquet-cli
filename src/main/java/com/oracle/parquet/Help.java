package com.oracle.parquet;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import org.slf4j.Logger;

// import org.apache.parquet.cli.Help;

@Parameters(commandDescription = "Retrieves details on the functions of other commands")
public class Help extends org.apache.parquet.cli.Help {

  public Help(JCommander jc, Logger console) {
    super(jc, console);
  }
  List<String> helpCommands = Lists.newArrayList();  
}