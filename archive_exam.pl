#!/usr/bin/perl 
#
#   $URL: https://intrarad.ucsf.edu/svn/rad_software/surbeck/brain/DB/nelson_ncvi/trunk/image_archive_scripts/archive_exam $
#   $Rev: 39691 $
#   $Author: jasonc@RADIOLOGY.UCSF.EDU $
#   $Date: 2017-05-19 13:13:11 -0700 (Fri, 19 May 2017) $
#

use strict; 
use File::Spec;
use Fcntl qw(:flock);
use Pod::Usage;
use File::Copy;
require SET_BRAIN_MODULES; 
require SET_PACS_MODULES; 
use MRSC_CSI_v6;
use Cwd;
use Cwd 'abs_path';
use Getopt::Long;
Getopt::Long::config("bundling");


sub get_tmp_archive_dir($$); 
sub chdir_print($); 


my $cwd = $ENV{'PWD'};
my $script = $0;
my $DEV = 0;
if ( $script =~ m/.*\.dev$/ ) {
    $DEV = 1;
}
my @orig_args = @ARGV;


my ($examNum, $tnum, $retainLocalArchive, $help, $pswd);
my $DEBUG = 0; 

my $db_instance     = BRAIN_DB_UTILS::dbutils_get_db_instance();
my $domain          = $nelson_ncvi_conf::domain{$db_instance};
my $patientIDPrefix = $nelson_ncvi_conf::patientIDPrefix{$db_instance};
my $examIDPrefix    = $nelson_ncvi_conf::examIDPrefix{$db_instance};

my @podSections;
if ( $domain eq "Brain" ) {
    @podSections = ("NAME_BRAIN", "SYNOPSIS_BRAIN", "DESCRIPTION", "EXAMPLES_BRAIN" );
} elsif ( $domain eq "MS" ) {
    @podSections = ("NAME_MS", "SYNOPSIS_MS", "DESCRIPTION", "EXAMPLES_MS" );
}
@podSections = ("NAME", "SYNOPSIS", "DESCRIPTION", "EXAMPLES" );


GetOptions(
           "e=s"                => \$examNum,
           "r"                  => \$retainLocalArchive,
           "raw_prefix=s"       => \my $rawPrefix, 
           "study=s"            => \my $study_tag_in,
           "meta"               => \my $metaOnly,
           "p=s"                => \$pswd,
           "D"                  => \$DEBUG,
           "h"                  => \$help
) or pod2usage(-verbose=>99, -exitval=>2, -sections => [ @podSections ] );


if (defined $help ) {
    pod2usage(-verbose=>99, -exitval=>0, -sections => [ @podSections ] );
}


if (!defined $examNum ) {
    my @examDirs = glob("E*"); 
    my $numEDirs = scalar @examDirs;  
    if ( $numEDirs != 1 ) { 
        print "=========================================\n"; 
        print "ERROR, found more than one exam directory: @examDirs\n"; 
        print "=========================================\n"; 
        exit(1); 
    }
    $examNum = $examDirs[0]; 
    chomp $examNum; 
} 



my $start_dir = getcwd();
$start_dir = File::Spec->rel2abs($start_dir);

if ($DEBUG) {
    BRAIN_DB_UTILS::dbutils_turn_debug_on();
    DCM4CHEE::dcm4chee_debug_on();
}


##########################
#   log the job start:
##########################
my $log_dir = getcwd();
$log_dir = File::Spec->rel2abs($log_dir);
csi_set_parent_dir($log_dir);
my @noPwdArgs; 
for (my $index = 0; $index < @orig_args; $index++ ) {
    if ( $orig_args[$index]=~ m/\-p/ ) {
        push ( @noPwdArgs, $orig_args[$index] ); 
        push ( @noPwdArgs, "pswd" ); 
        $index++;  
    } else {
        push ( @noPwdArgs, $orig_args[$index] ); 
    }
}
csi_log_start($script, @noPwdArgs);

#
#   Get t-num from exam data: 
#
my $getPIcmd = "get_patient_id"; 
$getPIcmd  = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $getPIcmd );
$getPIcmd .= " -e $examNum -c"; 
if ($DEBUG) {
    print "$getPIcmd\n"; 
}
`$getPIcmd`; 
if ( !defined $tnum ) {
    my $getcmd = "get_exam_id"; 
    $getcmd  = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $getcmd );
    $getcmd .= " -e $examNum -c"; 
    if ($DEBUG) {
        print "pwd: " . cwd . "\n"; 
        print "$getcmd\n"; 
    }
    my @tnumArray = `$getcmd`; 
    foreach my $f (@tnumArray) {
        if ( $f =~ m/${examIDPrefix}_number: (\d+)/) {
            $tnum = $1; 
        }
    }
}
if ( !defined $tnum ) {
    print "Could not determine ${examIDPrefix}-number\n";  
    exit(1); 
}
print "tnum: ${examIDPrefix}${tnum}\n"; 


#
#   If data has been found, check image attributes and try to lookup b-number/t-number in DB
#
my $accessionID = lookupBrainDBIDS($tnum);
my $tmp_archive_dir;

if (defined $accessionID )  {

    $tmp_archive_dir = get_tmp_archive_dir("t$tnum", 1); 
    chdir_print( "$tmp_archive_dir" ); 

    #   make links to items to be archived: 
    my $examDir = "../${examNum}"; 
    if ( -d $examDir ) { 
        my $cmd = "ln -s $examDir"; 
        if ( system( "$cmd" ) ) {
            print "ERROR, could not make link: $cmd\n";     
            exit(1);
        }
    } else {
        print "ERROR, could not find DICOM exam dir to archive: $examDir\n";
        exit(1);
    }
    #gunzip dicom?
    my $gunzipCmd = "group_gunzip -r ${examDir}";  
    system( "$gunzipCmd"); 

    my $rawGlobDir = File::Spec->rel2abs("../");
    my $rawGlob = "${rawGlobDir}/${examIDPrefix}${tnum}*"; 
    if ( defined $rawPrefix ) {
        $rawGlob = "${rawGlobDir}/${rawPrefix}*"; 
    }
    my @rawFiles = glob("$rawGlob"); 
    if ($DEBUG) {
        print "Raw files to archive: $rawGlob\n"; 
        print "found: @rawFiles\n"; 
    }
    if ( ! @rawFiles ) { 
        print "=========================================\n"; 
        print "WARNING: Could not find raw files: ${rawGlob} \n";     
        print "=========================================\n"; 
    }

    foreach my $file ( @rawFiles ) {

        my $cmd = "ln -s $file"; 
        if ($DEBUG) {
            print "$cmd\n";
        }
        if ( system( "$cmd") ) {
            print "ERROR, could not make link: $cmd\n";     
            exit(1);
        }
    }

    my $cmd = "get_scanner_data.pl"; 
    $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
    if ( defined $metaOnly ) {
        $cmd .= " --meta "; 
    }
    $cmd .= " --archive_local ./ -a ${accessionID} "; 
    if ( @rawFiles ) {
        $cmd .= " --all_raw ";     
    }
    if ($DEBUG) {
        $cmd .= " -D "; 
    }
    if ( defined $study_tag_in) {
        $cmd .= " --study $study_tag_in "; 
    }


    print "\n$cmd\n\n";

    if ( defined $pswd ) {
        $cmd .= " -p $pswd ";
    }
 
    if ( system("$cmd") ) { 
        print "\n";
        print "###############################################\n"; 
        print "###############################################\n"; 
        print "\n";
        print "ERROR ARCHIVING EXAM: $cmd\n";     
        print "\n";
        print "###############################################\n"; 
        print "###############################################\n"; 
        print "\n";
        exit(1);
    }
    
}

chdir_print("$cwd"); 
if ( !defined $retainLocalArchive ) {
    my $cmd = "cp -r ${tmp_archive_dir}/${patientIDPrefix}* ."; 
    print "$cmd\n";
    system("$cmd"); 
} 
my $cmd = "rm -rf ${tmp_archive_dir}"; 
print "$cmd\n";
system("$cmd"); 

print "\n";


##########################
#   log the job end:
##########################
my $log_dir = getcwd();
$log_dir = File::Spec->rel2abs($log_dir);
csi_set_parent_dir("$log_dir");
csi_log_end($script, @noPwdArgs);


########################################################################
########################################################################
#   SUBROUTINES
########################################################################
########################################################################


#   Given an AccessionNumber, lookup the brain_id and t-number from 
#   the brain DB.  If the t-number doesn't exist generate one.  Data should be moved
#   to a local directory and deidentified with the t-number.  
sub lookupBrainDBIDS($)
{
    my ($tnum) = @_; 

    my $accessionID;
    if (defined $tnum ) {
        #lookup accession number to search for
        $accessionID = BRAIN_DB_UTILS::dbutils_get_accession_from_tnum($tnum);
        if ($accessionID eq "") {
            print "===============================\n";
            print "ERROR:  could not find accession number for ${examIDPrefix}number($tnum)\n";
            print "===============================\n\n";
            exit(1); 
        }
    }
    return $accessionID; 
}


#   
#   Get tmp data transfer dir 
#   remove the old one if necessary
#   
sub get_tmp_archive_dir($$)
{
    my ($tnum, $deleteDir) = @_; 
    my $cwd = getcwd();
    my $tmp_archive_dir = File::Spec->rel2abs("${cwd}");
    $tmp_archive_dir = "${tmp_archive_dir}/archive_dir_tmp_$tnum";  
    if ($deleteDir) {
        if ( -d $tmp_archive_dir ) {
            my $cmd = "rm -rf  $tmp_archive_dir" ; 
            if (system( $cmd ) ) {
                print "===============================\n";
                print "Could not delete tmp archive dir: \n";
                print "$tmp_archive_dir \n";
                print "===============================\n\n";
                exit(1);    
            }
        }
        mkdir ( $tmp_archive_dir ); 
        my $chgrp_cmd = "chgrp dicom  $tmp_archive_dir"; 
        `$chgrp_cmd`; 
        my $chmod_cmd = "chmod g+s  $tmp_archive_dir"; 
        `$chmod_cmd`; 
    }

    return ${tmp_archive_dir}; 
}


#
#   Creates an output file in the $tmp_transfer_dir
#
sub create_output_file($$) 
{
    my ($path, $fn) = @_; 
    my $output_file = "$path/$fn"; 

    if (! -e $output_file) {
        open(LOGFILE, "> $output_file");
        chmod (0775, $output_file);
        close(LOGFILE);
    }
    open(LOGFILE, "> $output_file");
    close(LOGFILE);
    
    return $output_file; 
}
    

sub chdir_print($)
{
    my ($targetDir) = @_; 
    chdir( "$targetDir" ); 
    print "cwd: " . cwd . "\n"; 
}



###############################################################################
#
#   POD Usage docs
#
###############################################################################

=head1 NAME

    archive_exam    

=head1 SYNOPSIS

    archive_exam [ -e Exam_Number ] [ --meta ] [ -Dh ]

        -e              exam_number     Exam number containing DICOM images ("E# dir)
        --raw_prefix    prefix          Prefix for raw files.  By default this will be the private
                                        exam number (mse#, t#, etc).  Example --raw_prefix a1234 will search 
                                        for raw files a1234* in cwd. 
        --study         studyTag        the study the exam belongs to
        --meta                          ONly upload meta data (imaging attributes), do not send DICOM to PACS. 
        -r                              retain local deidentified copy of archive 
        -D                              turn on debugging 
        -h                              Print help message.

=head1 DESCRIPTION

Program to archive existing exam. May be run from t folder. Uses E# dir from current directory


=head1 EXAMPLES   

archive_exam -e E987


=cut

