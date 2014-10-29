#!/svw/svw_app/perl/rel/bin/perl
###Version number in the header sucks unless it is automatically updated. Add a 
###revision history instead.
#*********************************** Header ************************************
#
#      Name: check_if_partition_sort_failed_and_block_billrun
#  Language: Perl script 
#    Author: Avinash Sarapali
#
#
#
#******************************** Description **********************************
#
#  Checks if partition sort has failed. It it has failed, it starts a B
#  BILL_RUN_BLOCKER task which blocks the billrun from starting.
#
#********************************************************************************



#********************************************************************************
#       CORE PERL LIBRARIES
#********************************************************************************
use strict;         #makes sure that all variables are declared
use FileHandle;     #use only if you use file handler or do ataiSystem
use Data::Dumper;   #Easy and visual display of sturctures like hashes & arrays
use POSIX;          #POSIX functions
use DBI;            #DBI interface for database access
use Getopt::Long;   #Getops for handling command line arguements



#********************************************************************************
#       PROGRAM LIBRARIES
#********************************************************************************
use lib '/svw/svwprd1b/imp/ops/mon/live';                #DB connection libray path
use dbCredentials;                                       #DB connection library
use lib '.',
        $ENV{ATAI_LOC_SERVER_LIB},
        $ENV{ATAI_REL_SERVER_LIB},
        $ENV{ATA_REL_SERVER_LIB};                        #SV libraries needed to start task
use ataierr qw(:std :SIL_GENERAL);                       # Add message files as required



#********************************************************************************
#      Global CONSTANTS 
#********************************************************************************
my $BILL_RUN_BLOCKER_SCHEDULE_TYPE = 16020842;
my $MODULE_NAME                    = 'check_if_partition_sort_failed.pl';
my $EXIT_SUCCESS                   = 0;
my $EXIT_INCORRECT_ARGS            = 0;


#********************************************************************************
#      Global variables 
#********************************************************************************
my $dbh;                   # Database handler (SingleView)
my $gParameters;           # Global variable contains all input parameters
my $gTaskQueueId;          # Stores received Task queue of the task
my $gEffectiveDate;        # Stores effective date  of the task
my $statementHandle;       # Hash Reference for all prepared statment handles

# These can only be used in this file.
use vars qw(
$opt_help
$opt_h
);



#********************************************************************************
#      Connect to the Database
#********************************************************************************
sub dbConnect
{
    my ($dbName, $dbUser, $dbPwd) = &dbCredentials::get_db_info('BILLING_ADC');

    $dbh = DBI->connect("DBI:Oracle:$dbName", "$dbUser", "$dbPwd", {AutoCommit => 0})
        or die $dbi::errstr;
}



#********************************************************************************
# Report errors in sql
#********************************************************************************
sub sqlError
{
    my ($error, $params) = @_;

    return sprintf("%s , %s", $error, ''.Data::Dumper->Dump([$params], [qw(Parameters)]));
}



#********************************************************************************
# prepares a hash of all sqls
#********************************************************************************
sub prepareSqlHash
{
    my $sqls;
    my $sqlType;

    $sqls->{SORT_TMP_TABLE} = <<EOS;
      SELECT TABLE_NAME FROM TABLE(MONITOR_PARTITION_SORT.GET_TMP_PARTITION_TABLES)        
EOS

    # Preparing all SQL statement
    eval 
    {
        foreach  $sqlType (keys %{$sqls}) 
        {
            $statementHandle->{$sqlType} = $dbh->prepare($sqls->{$sqlType}) 
                or die(sprintf("Unable to prepare query name '%s' error '%s'", $sqlType, $dbh->errstr()));
        }
    }
}



#*******************************************************************************************************************
# Takes a prepared DBI statment handle for a query that returns only one column
# Function that returns reference to a single dimensional array-
#*******************************************************************************************************************
sub extractSingleColumnFromDbToArray
{
    my ($inputStatementHandle,$inputArrayRef) = @_;
    my $tableEntry;
    my $item;

    $inputStatementHandle->execute() or die(sqlError($dbh->errstr()));

    while ($tableEntry = $inputStatementHandle->fetchrow_array())
    {
        @{$inputArrayRef}[$item] = $tableEntry; 
        $item = $item + 1;
    }
}



#*******************************************************************************************************************
# Whenever partition sort fails and leaves a temp table, only then do we need to recover and not start the bill run. 
# If partition sort fails and does not leave # the temp tables then it is nothing to recover. 
# Hence this functions decides whether partition sort failed by looking for temp tables
#*******************************************************************************************************************
sub checkAndReportPartitionSortFailed
{
    my $numberOfTables = 0;
    my @tmpTablesArray;

    extractSingleColumnFromDbToArray($statementHandle->{SORT_TMP_TABLE}, \@tmpTablesArray);

    $numberOfTables = scalar @tmpTablesArray;

    #We only do something if we find tables
    if ( $numberOfTables > 0 )
    {
        print "Total number of tmptables found  =  ".$numberOfTables. "\n";
        print Dumper(@tmpTablesArray)." \n"; 
    }
    else { print ("************No Tables Found************\n");   }

    $statementHandle->{SORT_TMP_TABLE}->finish();

    return $numberOfTables;
}



#********************************************************************************
# This is to make sure that the task never finishes unless spcifically stopped
##********************************************************************************
sub sleepForever
{
    print "Sleeping Forever\n";
    while (){}
}



#********************************************************************************
sub usage 
{
    print ("USAGE : check_if_partition_sort_failed_and_block_billrun.pl [-h][-help]\n");
    print ("If no parameters (default task_queue_id and effective_date) are supplied then the script \n");
    print ("checks for failed partitions and if found runs forever\n");
}



#***********************************************************************************
# validates input parameters.. At the moment only -h and -help and two default parms
#***********************************************************************************
sub validateInputArguements
{
    my $exitCode;
    if (($#ARGV != 2 ) or !&GetOptions("help", "h" )) 
    {
        usage();         
        exit($EXIT_INCORRECT_ARGS);
    }

    if ( $opt_h or $opt_help ) 
    {
        usage();         
        exit($EXIT_SUCCESS);
    }
}



#********************************************************************************
# validates parameters, Gets input parametes, initiates database connection 
# and prepares sqls 
#********************************************************************************
sub initialize
{
    validateInputArguements();

    # Get task queue_id & effective date
    $gTaskQueueId   = $ARGV[0];
    $gEffectiveDate = $ARGV[1];

    # Force output after every line (keeps stderr and stdout in sync)
    autoflush STDOUT 1;
    autoflush STDERR 1;

    dbConnect();

    prepareSqlHash(); 

    # Sort hash key
    $Data::Dumper::Sortkeys = 1;
}



#********************************************************************************
#  Logs alert using utility.
#********************************************************************************
sub logAlert
{
    my $PARTITION_SORT_ERROR = 3002;
    my $cmd = 'send_alert_to_alertlog.pl '.' -error_code '.$PARTITION_SORT_ERROR;
    my @cmdOutput = `$cmd`;
    print Dumper(@cmdOutput)." \n"; 
}


#********************************************************************************
#********************************************************************************
#MAIN
#********************************************************************************
#********************************************************************************

sub main
{ 
    initialize();

    my $numberOfTmpTablesFound = checkAndReportPartitionSortFailed();

    if ($numberOfTmpTablesFound > 0) 
    {
        logAlert();
        sleepForever();
    }

    $dbh->disconnect;
}

main();

#END
