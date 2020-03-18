## Launch BLooM

#### build code
- Download code
- Do a maven build for generate tar file

#### Untar binary
    cd /app/bloom/INSTALL
    tar -xvf bloom-core-1.0.0-SNAPSHOT-dist.tar

#### Create symlink for installation- optional
    ln -s /app/bloom/INSTALL/bloom-core-2.0.0-SNAPSHOT bloom
  
#### Execute

    <bloom_HOME>/bin/BLooMLauncher.sh  
    
    usage: bloom
     -tn,--tableName<arg>   It should be the name of the table where the data has to be loaded
     -h,--help                     Show Help.
     -ip,--input <arg>             Directory location where data file from
                                   ESODL will be dropped
     -it,--inputType <arg>         This indictes the type of file like
                                   csv,hive table etc..
     -rt,--refreshType <arg>       This indicates the type of
                                   load-FULL_REFRESH,UPSERT
     -ar,--archival <arg>          This indicates if archival of csv files is needed or not

#### For Full-REFRESH

    /app/bloom/INSTALL/bloom/bin/BLooMLauncher.sh \ or bloom
        -tn account_fact  \
        -ip /staging/AccountMaster.csv \
        -rt Full-REFRESH \
        -it csv
        -tt rowstore
        -ar true

#### For UPSERT

    /app/bloom/INSTALL/bloom/bin/BLooMLauncher.sh \ or bloom
        -tn account_fact  \
        -ip /staging/AccountMaster.csv \
        -rt UPSERT \
        -it csv
        -tt rowstore
        -ar true

#### Execute command for processing multiple files from a directory

    /app/bloom/INSTALL/bloom/bin/BLooMFileProcess.sh \
    -tn account_fact  \
    -ip /warehouse/bloom/job/input/ \
    -rt Full-REFRESH \
    -it csv
    -ar true
    
    