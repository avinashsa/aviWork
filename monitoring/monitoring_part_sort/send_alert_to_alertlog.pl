#! /usr/bin/env perl
#*********************************** Header ************************************
#
#      Name: Perl script to add alert to alert log (adcread.alerts table)
#  Language: Perl script 
#    Author: Avinash Sarapali
#
#******************************** Description **********************************
#
#  find error details from error config
#  Logs error to adcread.alerts table
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
use lib "/svw/svwprd1b/imp/ops/server/lib";              #needed for read error library
use readError;                                           # used to read error codes from library
use dbCredentials;                                       #DB connection library
use lib '.',
        $ENV{ATAI_LOC_SERVER_LIB},
        $ENV{ATAI_REL_SERVER_LIB},
        $ENV{ATA_REL_SERVER_LIB};                        #SV libraries needed to start task
use ataierr qw(:std :SIL_GENERAL);                       # Add message files as required



#********************************************************************************
#      Global CONSTANTS 
#********************************************************************************
my $MODULE_NAME                    = 'send_alert_to_alertlog.pl';
my $EXIT_SUCCESS                   = 0;
my $EXIT_INCORRECT_ARGS            = 1;
my $EXIT_NO_ERROR_CODE_FOUND       = 2;



#********************************************************************************
#      Global variables 
#********************************************************************************
my $now;                   # Current time string 
my $dbh;                   # Database handler (SingleView)
my $gParameters;           # Global variable contains all input parameters
my $gTaskQueueId;          # Stores received Task queue of the task
my $gEffectiveDate;        # Stores effective date  of the task
my $statementHandle;       # Hash Reference for all prepared statment handles

# These can only be used in this file.
use vars qw(
$opt_help
$opt_h
$opt_error_code
);
my %errorCodes;            #Hash containing all error codes and their descriptions



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

    $sqls->{ADD_ALERT} = <<EOS;
    BEGIN
        ADCREAD_PKG.NEW_ALERT('ERR_CODES',
                      :inAlertId,
                      adcread_pkg.alertError,
                      :inText,
                      :inHref);
    END;
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



#********************************************************************************
#Logs alert if error code is found  
#********************************************************************************
sub logAlert
{
    if(!$errorCodes{$opt_error_code}{PROCESS}) 
    {
        print("Error Code $opt_error_code not found \n"); 
        exit($EXIT_NO_ERROR_CODE_FOUND);
    }

    $now = localtime;
    $statementHandle->{ADD_ALERT}->bind_param(":inAlertId",    $opt_error_code."_$now");
    $statementHandle->{ADD_ALERT}->bind_param(":inText",      $errorCodes{$opt_error_code}{ERROR_MESSAGE}.":".$errorCodes{$opt_error_code}{ERROR_ACTION});
    $statementHandle->{ADD_ALERT}->bind_param(":inHref",       "");

    $statementHandle->{ADD_ALERT}->execute or die $dbh->errstr;
}



#********************************************************************************
sub usage 
{
    print ("USAGE : send_alert_to_alertlog [-h][-help] -error_code ERR_CODE\n");
}



#***********************************************************************************
# validates input parameters.. At the moment only -h and -help and two default parms
#***********************************************************************************
sub validateInputArguements
{
   my $exitCode;
    if ( ($#ARGV != 1 ) || !&GetOptions("help", "h", "error_code=n" )) 
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

    # Force output after every line (keeps stderr and stdout in sync)
    autoflush STDOUT 1;
    autoflush STDERR 1;

    dbConnect();

    prepareSqlHash(); 

    readErrorCodes(0,\%errorCodes);
    
    # Sort hash key
    $Data::Dumper::Sortkeys = 1;
}



#********************************************************************************
#********************************************************************************
#MAIN
#********************************************************************************
#********************************************************************************

sub main
{ 
    initialize();

    logAlert();

    $dbh->disconnect;
}

main();

#END
