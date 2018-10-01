#!/usr/bin/perl 
#
#   $URL: https://intrarad.ucsf.edu/svn/rad_software/surbeck/brain/DB/nelson_ncvi/trunk/image_archive_scripts/get_scanner_data $
#   $Rev: 40608 $
#   $Author: bolson@RADIOLOGY.UCSF.EDU $
#   $Date: 2018-01-19 11:55:53 -0800 (Fri, 19 Jan 2018) $
#

use threads;
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


sub getHash( $ ); 
sub checkForBDir(); 
sub getRawSeriesDescription(@); 
sub getRawFileSize(@); 
sub upload_data_attributes_to_DB($); 
sub get_tmp_transfer_dir($); 
sub get_tmp_tnum_dir($); 
sub update_db_info(); 
sub post_update($$$$$); 
sub convert_raw_to_dicom(); 
sub get_number_of_dcm_images($); 
sub getNewExamID(); 
sub getNewBrainID(); 
sub check_pswd(); 
sub getLocalPathToDICOMData($);
sub getLocalPathToRawData($);
sub writeDaemonJobFile($$@); 
sub monitorDaemonJobsSCP($$$); 
sub monitorDaemonJobsQR($$$); 
sub sortRawFilesByTime(@); 
sub post_retrieve($$); 


my %scpDB;              #   values for DICOM files from DB FILE 
my %rawDB;              #   values for Raw   files from DB FILE 
my %dicomHash;          #   new values for DICOM files to be written to file
my %rawHash;            #   new values for Raw files to be written to file
my %copiedRawFileHash;  #   hash of names of raw files that have already been copied.
my $bnum; 
my $tnum; 
my $pswd; 
my $cwd = $ENV{'PWD'};
my $script = $0;
my $DEV = 0;
if ( $script =~ m/.*\.dev$/ ) {
    $DEV = 1;
}
my @orig_args = @ARGV;


$DEBUG = 0; 
$foundDICOM = 0; 
$foundRaw = 0; 

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

#   recognized raw file series descriptions: 
%rawAcqTypes = (
        'asset cal si'                   => '_acsi',
        'asset calibration si'           => '_acsi',
        'repeat'                         => 'repeat',
        'swi'                            => '_swi',
        'gre obl'                        => '_gre',
        'gre ax'                         => '_gre',
        'gre cor'                        => '_gre_cor',
        'gre2e'                          => '_gre2e',
        'lac edit flyback'               => '',
        'lactate edit flyback long echo' => '',
        'flyback'                        => '',
        'mrsi'                           => '',
        'short-echo'                     => '_short',
        'short'                          => '_short', 
        'svs-2d left caudate'            => '_svs_2d_left_caudate',
        'svs-2d right caudate'           => '_svs_2d_right_caudate',
        'svs-2d left auditory cortex'    => '_svs_2d_left_auditory',
        'svs-2d right auditory cortex'   => '_svs_2d_right_auditory',
        'b1 map'                         => '_b1map',
        'svs'                            => '_svs',
        'hos'                            => '_hos',
        'se 3d csi'                      => '',
        'se 2d csi'                      => '',
        'presse 2d csi'                  => '',
        'multiband dti noddi'            => '_multiband_dti_noddi', 
        'ute'                            => '_ute', 
        'cpress'                         => '', 
        'c13 calibration'                => '_c13_calibration',
        'c13 patient'                    => '_c13_patient',
        'c13 phantom'                    => '_c13_phantom',
        'c13'                            => '_c13',
        'cosy'                           => '_cosy',
        'svs 1 in t2l \(2hg\)'           => '_svs',
        'svs te=97ms'                    => '_svs_asym',
        'svs te=68ms'                    => '_svs_edit',
        'bbepi'                          => '_bbepi',
        'ax 3d swan'                     => '_swan',
        'slaser'                         => '_slaser'

    ); 

GetOptions(
           "a=s"                => \$accessionId,
           "t=s"                => \$tnum,
           "all_raw"            => \$allRaw,
           "no_snd"             => \$noSnd,
           "archive_local=s"    => \$localArchive,
           "p=s"                => \$pswd,
           "study=s"            => \$study_tag_in,
           "c=s"                => \$convertConfigFile,
           "meta"               => \my $metaOnly,
           "recognized_raw"     => \my $recognized_raw,
           "D"                  => \$DEBUG,
           "h"                  => \$help
) or pod2usage(-verbose=>99, -exitval=>2, -sections => [ @podSections ] );


#   by default get all raw files
$allRaw = 1; 

if ( defined $help ) {
    pod2usage(-verbose=>99, -exitval=>0, -sections => [ @podSections ] );
}
if ( defined $recognized_raw ) {
    print "===================================================\n";     
    print "Raw file with the following series will be archived\n";     
    print "===================================================\n";     
    foreach (keys %rawAcqTypes) {
        printf ("%35s => %25s\n",  $_,  $rawAcqTypes{$_}); 
    }
    print "===================================================\n";     
    exit(1); 
}
if ( $domain eq "Brain" && !defined $study_tag_in ) {
    print "===================================================\n";     
    print "ERROR: Missing required argument \"study\".\n";     
    print "===================================================\n";     
    pod2usage(-verbose=>99, -exitval=>1, -sections => [ @podSections ] );
}

if (defined $localArchive) {
    $localArchive = File::Spec->rel2abs("${localArchive}");
}

if (defined $convertConfigFile) {
    $convertConfigFile = File::Spec->rel2abs("${convertConfigFile}");
    if( ! -e $convertConfigFile ) {
        print "ERROR: config file does not exist:$convertConfigFile\n";
        exit(1);
    }
}

if ($DEBUG) {
    BRAIN_DB_UTILS::dbutils_turn_debug_on();
    DCM4CHEE::dcm4chee_debug_on();
}

my $lookupID; 
validateInput();

my $start_dir = getcwd();
$start_dir = File::Spec->rel2abs($start_dir);

##########################
#   log the job start:
##########################
my $log_dir = getcwd();
$log_dir = File::Spec->rel2abs($log_dir);
csi_set_parent_dir($log_dir);
csi_log_start($script, @orig_args);
chmod( 0770, "$log_dir/Logfile"); 

my $ncvi_log_dir= "/data/dicom/index/Logfiles/ncvi_audit";
csi_set_parent_dir($ncvi_log_dir);
csi_log_start($script, @orig_args);


#
#   authenticate access to PACS
#
if ( defined $pswd ) {
    DCM4CHEE::dcm4chee_set_pswd($pswd);
} else {
    DCM4CHEE::dcm4chee_prompt_pswd();
}
my $private_pswd = DCM4CHEE::dcm4chee_get_pswd();
check_pswd(); 

$tmp_tnum_dir = get_tmp_tnum_dir(1); 
chdir($tmp_tnum_dir); 
$tmp_transfer_dir = get_tmp_transfer_dir(1); 


$EXAM_NUMBER = "EXAM_UNK"; 
if (defined $accessionId ) {
    #   Get the exam number for later use:
    #   As there may be more than one exam, use a generic one: 
    $EXAM_NUMBER = "EXAM${accessionId}"; 
} 

if ( !defined $noSnd ) {

    if (defined $accessionId ) {

        my $dbFile = "$DCM4CHEE_config::scpDB";
        %dicomHash = getHash($dbFile); 

        $dbFile = "$DCM4CHEE_config::rawDB";
        %rawHash = getHash($dbFile); 

        if (defined $localArchive) { 
            ($foundDICOM, @dicomImages) = getLocalPathToDICOMData($localArchive);
            ($foundRaw, @rawData) = getLocalPathToRawData($localArchive);
        } else {
            ($foundDICOM, @dicomImages) = getPathToAccessionNumber($accessionId, %dicomHash);
            ($foundRaw, @rawData) = getPathToAccessionNumber($accessionId, %rawHash);
        }

        #   Sort raw files by time.  Some multi-pfile acquisitions need to be in correct time order: 
        @rawData = sortRawFilesByTime(@rawData); 

        if ( $foundDICOM == 0 && $foundRaw == 0 ) {

            print "===============================\n";
            print "Could not locate data for $lookupID\n";
            print "===============================\n\n";
            exit(1);

        } else {

            #
            #   Check if still downloading
            #
            $cmd = "find @dicomImages -name '*.DCM' | wc"; 
            print "is downloading finished: $cmd\n"; 
            $downloading = 1; 
            $num = `$cmd`; 
            print "\tNUM downloaded: $num" ; 
            $count = 0;  
            while ($downloading ) {
                sleep(5); 
                $num_tmp = `$cmd`; 
                print "\tNUM downloaded: $num_tmp" ; 
                if ($num_tmp == $num ) {
                    $downloading = 0; 
                } else {
                    $num = $num_tmp;
                }
                $count++; 
                if ($count == 10 ) {
                    print "try again later, still downloading\n"; 
                    exit(1); 
                }
            }

            chdir( $cwd ); 

            if ( -d $tmp_transfer_dir) {

                ###################################
                #   DICOM images
                ###################################
                print "DICOM: \n";
                $arbitraryDICOMSeriesNumber = 0; 
                foreach $item (@dicomImages) {
                    $foundDICOM = 1; 
                    $fileNameTmp = getFileNameFromPath($tmp_transfer_dir); 
                    printf( "\tcopying %-60s to %-60s\n", $item, $fileNameTmp);

                    $exam_dir = "${tmp_transfer_dir}/${EXAM_NUMBER}";
                    DCM4CHEE::dcm4chee_mkdir_group( "${exam_dir}" ); 
                    chdir (${exam_dir});     
                    @series = glob("$item/*"); 
                    foreach $link ( @series ) {

                        $arbitraryDICOMSeriesNumber++; 
                        $cmd = "ln -s $link $arbitraryDICOMSeriesNumber";
                        if (system("$cmd")) { 
                            print "===============================\n";
                            print "Failed to copy $item to $tmp_transfer_dir\n";
                            print "===============================\n\n";
                            exit(1);    
                        }
                    }
                    chdir (${tmp_transfer_dir});     
                }

                ###################################
                #   RAW files 
                ###################################
                print "RAW:   \n";
                foreach $rawFile (@rawData) {
                    $foundRaw = 1; 
                    $rawFileCopy = mapRawFileName( $rawFile ); 
                    if ( $rawFileCopy ne "" ) {
                        $fileNameTmp = getFileNameFromPath($rawFileCopy); 
                        printf("\tcopying %-60s to %-60s\n", $rawFile, $fileNameTmp);
                        $cmd = "ln -s $rawFile $fileNameTmp";
                        system($cmd);
                        #copy($rawFile, $rawFileCopy) or die "Failed to copy $rawFile to $rawFileCopy: $!\n";
                        copyDatFiles ($rawFile, $rawFileCopy); 
                    } else {
                        print "\tNot copying $rawFile\n\n";
                    }
                }

            } else {
                print "===============================\n";
                print "Could not make directory to copy data to: $tmp_transfer_dir\n";
                print "===============================\n\n";
                exit(1);
            }
        }

        print "\n";
    }    
}


#
#   If data has been found, check image attributes and try to lookup b-number/t-number in DB
#
lookupBrainDBIDS();


checkForBDir();    


#
#   Now, convert raw files to DICOM files and add to exam directory. 
#
if ( !defined $noSnd ) {
    convert_raw_to_dicom(); 
}


#
#   Now, upload info about identified files (DICOM and RAW) to DB
#
if ( !defined $noSnd ) {
    upload_data_attributes_to_DB( $tmp_transfer_dir );
    if ( defined $metaOnly ) {
        print "WARNING: Only uploaded meta data, exiting.\n"; 
        exit(0); 
    }
}


#
#   Now, archive the data in PACS: 
#

$exam_dir = "${tmp_transfer_dir}/${EXAM_NUMBER}";
if ( ! $DEV ) {
    my $number_of_dicom_files_to_archive;
    my $number_of_dicom_files_from_archive;
    if ( !defined($noSnd)) {
        archive_exam( $exam_dir);
    }
} else {   
    print "WARNING!!! Dev run, will not archive exam in brain PACS!\n";
}

#
#   Now, retrieve the data from PACS for verification: 
#
#retrieve_exam( $tnum, $exam_dir );


#
#   Now, convert the data to idf and raw files, and deidentify everything in the process. 
#
convert_exam($convertConfigFile); 

post_retrieve($examDir, $tnum); 

print "\n";


##########################
#   log the job end:
##########################
#my $log_dir = getcwd();
#$log_dir = File::Spec->rel2abs($log_dir);
#csi_set_parent_dir("$log_dir/$tnum");
csi_set_parent_dir($log_dir);
csi_log_end($script, @orig_args);

csi_set_parent_dir($ncvi_log_dir);
csi_log_end($script, @orig_args);

print "=====================================\n";
print " Congratulations, your job completed.\n"; 
print "=====================================\n";


########################################################################
########################################################################
#   SUBROUTINES
########################################################################
########################################################################


#
#   Initializes hash of available data from index files:
#
sub getHash( $ )
{
    my ($dbFile) = @_; 
    my %dbHash; 

    open(DB, "<", "$dbFile") or die "Couldn't read file $dbFile $!\n";
    @db = <DB>; 
    close(DB);

    foreach $line ( @db ) {
        if ( $line =~ m/\s+(\S+)\s+(\S+)/ ) {
            $key = $1; 
            $value = $2; 
            $dbHash{$key} = $value;  
        }
    }

    return %dbHash; 

}



#
#   Upload identified imaging attributes to brain DB
#       Raw and DICOM
#
sub upload_data_attributes_to_DB($)
{
    print "======================================= \n";
    print "Upload Exam Info To DB:                 \n";
    print "======================================= \n";
    print "\n";

    my ( $data_for_upload_dir ) = @_;

    ##############################
    #   DICOM data attributes
    ##############################
    BRAIN_DB_UTILS::dbutils_insert_imaging_attributes( "${data_for_upload_dir}/${EXAM_NUMBER}", "$tmp_transfer_dir" ); 


    ##############################
    #   Raw data attributes
    ##############################
    chdir (${data_for_upload_dir} ); 
    if ( $foundRaw == 1 ) { 
        my $output_file = BRAIN_DB_UTILS::dbutils_create_output_file("$tmp_transfer_dir", "insert_pfile_attributes.log");
        $cmd = "insert_pfile_attributes";
        $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
        $cmd .= " --pfile_dir  ${data_for_upload_dir}  >> $output_file 2>&1";
        if ($DEBUG) {
            print "$cmd\n";
        }
        if ( system("$cmd") ) {
            print "ERROR, could not upload pfile attributes to DB\n";
            exit(1);
        }
    }

    update_db_info(); 

}


#
#
#
sub getFileNameFromPath($)
{
    my ($path) = @_; 
    $file = $path;
    if ( $path =~ m/.*\/(\S+)\b/ ) {
        $file = $1; 
    }
    return $file; 
}


#
#   parse series description from raw file: 
#       series.se_desc =                fiddynamic2                     # Series Description
#
sub getRawSeriesDescription(@)
{
    my (@attributes) = @_;

    $seriesDescription = "";
    foreach $att (@attributes) {
        if ( $att =~ m/rhs\.se_desc\s+\=\s+(\S+.*)\s+.*/ ) {
            $seriesDescription = $1;
        }
    }

    return $seriesDescription; 
}


#
#   parse series description from raw file: 
#       hdr_rec.rdb_hdr_off_data =      145908    #  Byte offset to start of raw data (i.e size of POOL_HEADER)   
#       hdr_rec.rdb_hdr_raw_pass_size = 538968064
#
sub getRawFileSize(@)
{
    my (@attributes) = @_;

    $hdr_off_data = 0;
    $hdr_raw_pass_size = 0;
    foreach $att (@attributes) {
        if ( $att =~ m/rhr\.rdb_hdr_off_data\s+\=\s+(\d+)\s+.*/ ) {
            $hdr_off_data = $1;
        }
        if ( $att =~ m/rhr\.rh_raw_pass_size\s+\=\s+(\d+)\s+.*/ ) {
            $hdr_raw_pass_size = $1;
        }
    }

    return $hdr_off_data + $hdr_raw_pass_size; 
}



#
#
#
sub getLocalPathToDICOMData($)
{
    my ($dir) = @_;
    my $foundData = 0; 
    @data = glob("$dir/E*");
    if (@data) {
        $foundData = 1; 
    }
    return ($foundData, @data);
}


#
#   returns pfiles in name length order, longest first.  
#   Also renames pfiles and dat files to correspond to what
#   they would have been originally when pulled from the 
#   scanner, with the exception of the time stamp which is set
#   to 00. 
#
sub getLocalPathToRawData($)
{
    my ($dir) = @_;
    my $foundData = 0; 
    my @data; 
    @files    = glob("$dir/*");
    @datFiles = glob("$dir/*.dat $dir/*.xml");
    foreach  (@files) {
        #   file shouldn't be a directory or a link:
        if ( ! -d $_ ) {
            my $cmd = "svk_get_filetype -i $_ 2>/dev/null";
            my $out = `$cmd`;
            if ($out =~ m/GE pfile/) {
                push (@data, $_); 
                $foundData = 1; 
            }
        }

    }

    my @data_sorted = sort {length $b <=> length $a} @data;

    #   rename local raw and dat files: 
    foreach $pfile (@data_sorted) { 

        $pfileRoot = $pfile; 
        $pfileRoot =~ s/${dir}\/(.*)/$1/; 
        if ($DEBUG) {
            print "\n\npfileroot: $pfileRoot\n\n"; 
        } 

        $cmd = "/netopt/bin/local/svk_gepfile_reader --print_header -i $pfile | grep rawrunnum";

        $runNum = `$cmd`;
        $runNum =~ s/rhi.rawrunnum\s+=\s+(\d+)\s+.*/$1/;

        chomp $runNum; 

        $path = $pfile; 
        $path =~ s/(.*)\/.*/$1/; 

        my $timeStamp = 0; 
        my $leadingZero = "0";
        # This while loop is to account for series that are split into multiple pfiles
        while( -e "${path}/P${runNum}.7_$leadingZero$timeStamp" ) {
            $timeStamp++;
            if ( $timeStamp > 99 ) {
                print "ERROR: More than 100 pfiles found for run number:${runNum}\n";
                exit(1);
            } elsif( $timeStamp > 9 ) {
                $leadingZero = "";
            } 

        }
            
        my $newPfileName = "${path}/P${runNum}.7_$leadingZero$timeStamp";
        $cmd = "mv $pfile ${newPfileName}";
        push (@data_sorted_renamed, $newPfileName); 
        if ( system("$cmd") ) { 
            print "ERROR: could not rename raw file $cmd\n";
            exit(1); 
        }

        #   Now find and rename .dat files    
       
        for my $index ( 0 .. $#datFiles ) {
            $dat_file = $datFiles[$index]; 
            if ($dat_file =~ m/.*${pfileRoot}(.*)(\.dat|\.xml)/) {
                $suffix = $1; 
                $extension = $2;
                $cmd = "mv $dat_file ${path}/P${runNum}${suffix}${extension}_00";
                if ($DEBUG) {
                    print "$cmd\n"; 
                } 
                if ( system("$cmd") ) { 
                    print "ERROR: could not rename dat file $cmd\n";
                    exit(1); 
                }
                $datFiles[$index] = ""; 
                #if ($DEBUG) {
                    #foreach $tmp (@datFiles) {
                        #print "DATS: $tmp\n"; 
                    #}
                #}
            }
        }
    }

    return ($foundData, @data_sorted_renamed);
}



#
#   Find Imaging exam(s) based on specified search term:
#
sub getPathToAccessionNumber($%)
{
    my ( $accessionId, %dataHash ) = @_;
    my ( @data ); 
    $foundData = 0; 
    if ( defined $accessionId ) {
        foreach $key (keys %dataHash) {    
            if ( $key =~ m/^$accessionId[\|_\b]+/ ) {
                if ($DEBUG) {
                    print "$dataHash{$key}\n";
                }
                push(@data, $dataHash{$key} );     
                #print "$accessionId: $dataHash{$key}\n";
                $foundData = 1; 
            }
        }
    }

    return ($foundData, @data);
}


#
#   Validate input options: only one ID may be specified.
#
sub validateInput()
{
    $isInputValid = 1; 

    if ( !defined $accessionId && !defined $tnum ) {
        print "ERROR: specify a lookup identifier:\n\n";
        pod2usage(-verbose=>99, -exitval=>2, -sections => [ @podSections ] );
    }

    if ( defined $accessionId ) {
        if ( defined $mrn || defined $tnum || defined $bnum ) {
            $isInputValid = 0;    
        }
        $lookupID = $accessionId; 
    }
    
    if ( defined $tnum ) {
        if ( defined $accessionId || defined $mrn || defined $bnum ) {
            $isInputValid = 0;    
        }
        $lookupID = $tnum; 
    }

    if ( ! $isInputValid ) {
        print "ERROR: Only specify one lookup identifier:\n\n";
        pod2usage(-verbose=>99, -exitval=>2, -sections => [ @podSections ] );
    }
}


#   Given an AccessionNumber, lookup the brain_id and t-number from 
#   the brain DB.  If the t-number doesn't exist generate one.  Data should be moved
#   to a local directory and deidentified with the t-number.  
sub lookupBrainDBIDS()
{

    print "======================================= \n";
    print "Lookup $domain IDs:                     \n";
    print "======================================= \n";

    if (defined $accessionId) {

        #   See if the 
        ($bnum, $tnum) = BRAIN_DB_UTILS::dbutils_lookup_exam($accessionId);
        if ( $bnum != null && $bnum != null ) {
            print "\n";
            return; 
        }
        #
        #   Get bnum/tnum if one doesn't exist
        #
        getNewBrainID(); 
        getNewExamID(); 

    }


    if (defined $tnum ) {
        #lookup accession number to search for
        $accessionId = BRAIN_DB_UTILS::dbutils_get_accession_from_tnum($tnum);
        if ($accessionId eq "") {
            print "===============================\n";
            print "ERROR:  could not find accession number for tnumber($tnum)\n";
            print "===============================\n\n";
            exit(1); 
        }
        #   get the bnum  as well:  if there was a t-number, then there should be a b-number:
        ($bnum, $tnum) = BRAIN_DB_UTILS::dbutils_lookup_exam($accessionId);
    }
    print "\n";
}

sub getNewBrainID()
{

    #   Patient must be known.  If the bnum wasn't found, try 
    #   to find it from the imaging attributes directly.  It's 
    #   possible for example that the accession number/tnum just
    #   haven't yet been registered.
    if ( !defined $bnum || $bnum == null ) {

        my $cmd = "get_patient_id";
        $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
        $cmd .= " -e ${tmp_transfer_dir}/${EXAM_NUMBER} -c ";
        print "$cmd \n";
        $bnum = `$cmd`; 
        if ( $bnum =~ m/${domain} ID: ${patientIDPrefix}(\d+)/) {
            $bnum = "$1";
            print "patientID: ${patientIDPrefix}$bnum\n";
        } else { 
            print "===============================\n";
            print "ERROR:  patientID doesn't exist \n";
            print "===============================\n\n";
            exit(1);
        }
        
    }

    print "\n";
    return; 
}

sub getNewExamID()
{
    #   If the patient is recognized, but not the accession number for
    #   this exam, then generate a new t-number now.
    if ( !defined $tnum || $tnum == null ) {

        my $cmd = "get_exam_id";
        $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
        $cmd .= " -e ${tmp_transfer_dir}/${EXAM_NUMBER} -c ";
        print "$cmd \n";
        $tnum = `$cmd`; 

        if ( $tnum =~ m/${examIDPrefix}_number: (\d+)/) {
            $tnum = "$1";
            print "examID ${examIDPrefix}$tnum\n";
        } else { 
            print "===============================\n";
            print "ERROR:  examID doesn't exist and \n";
            print "could not be generated.\n";
            print "===============================\n\n";
            exit(1);
        }

    }
}


#   
#   Gets series description from raw file and tries to 
#   map to conventional naming.
#   If the file is not to be copied, then return an empty file name:
#   "".        
#   
sub mapRawFileName($)
{
    my ($rawFile) = @_;
    my $targetName; 

    #   search for recognized acquisition types:
    #       MRSI:  short, long, lac
    #       SWI
    #       ASSET CAL SI
    #
    #   Only P files that contain these strings
    #   in their Series Description defined by
    #   rdump will be copied.

    #   if importing all raw files, then add "_unk" identifier
    if ( defined $allRaw ) {
        $rawAcqTypes{'unknown'} = '_unk';
    }

    #   Get the series description from the raw file to be copied:
    #$cmd = "/netopt/bin/local/rdump -Ap $rawFile \| grep \"se_desc \\\|hdr_off_data \\\|hdr_raw_pass\"";
    $cmd = "svk_gepfile_reader --print_header -i $rawFile \| grep \"se_desc \\\|hdr_off_data \\\|raw_pass_size\"";
    @rawAttributes = `$cmd`;
    #print "RA: @rawAttributes\n"; 
    $seriesDescription = getRawSeriesDescription(@rawAttributes); 
    $seriesDescription = lc($seriesDescription);    
    
    if ($DEBUG) {
        print "SD: $seriesDescription\n";
    }

    #
    #   default raw file name mapping:
    #
    $rawFileName = getFileNameFromPath($rawFile); 
    $targetNameRoot = "${tmp_transfer_dir}/${rawFileName}_";
    $targetName = ""; 

    $raw_type; 
    my $is_data_size_unreliable = 0;
    foreach $type (keys %rawAcqTypes) {
        if ($seriesDescription =~ m/$type/) {
            # Check for noddi data to skip file size check
            if( $type =~ m/noddi/ || $type =~ m/bbepi/ ) {
                $is_data_size_unreliable = 1;
            }
            if ($DEBUG) { 
                print "map: $seriesDescription to $rawAcqTypes{$type} \n";
            }
            $targetName = "${targetNameRoot}$rawAcqTypes{$type}_1"; 
            $raw_type = "$rawAcqTypes{$type}_1";
            last;
        } 
    }
    
    #   if importing all raw and the series hasn't yet been found, then assign it 
    #   to unknown:
    if ( $targetName eq "" && defined $allRaw ) {       
        if ($DEBUG) { 
            print "map: $seriesDescription to $rawAcqTypes{'unknown'} \n";
        }
        $targetName = "${targetNameRoot}$rawAcqTypes{'unknown'}_1"; 
        $raw_type = "$rawAcqTypes{'unknown'}_1";
    }

    #   If this raw file is going to be copied, add it to the hash and possibly
    #   increment the file name ID if the name has already been used.    
    if ( $targetName ne "" ) {       

        #   Check that the file size is correct before copying: 
        $expected_raw_file_size = getRawFileSize(@rawAttributes);    
        $actual_raw_file_size = (stat($rawFile))[7];    
        if ( $actual_raw_file_size != $expected_raw_file_size) {
            if( $is_data_size_unreliable == 1 ) {
                print "===============================\n";
                print "WARNING: Data is NODDI or bbepi so file size \n";
                print "check is being bypassed!\n";
                print "PFile has incorrect size: $rawFile \n";
                print "$actual_raw_file_size != $expected_raw_file_size\n";
                print "===============================\n\n";
            } else {
                print "===============================\n";
                print "PFile has incorrect size: $rawFile \n";
                print "$actual_raw_file_size != $expected_raw_file_size\n";
                print "===============================\n\n";
                exit(1);    
            }
        }
            

        #   iterate through file numbers until we find one that hasn't 
        #   been used yet.  This enables indices to be used starting at 1
        #   for multiple file types, e.g. t123_swi_1 .. t123_swi_2... and 
        #   t123_acsi_1 ... t123_acsi_2...
        #print "\n\nrawtype:  $raw_type\n\n";
        #   get the type and number as the key to the hash, e.g. swi_1, swi_2, etc.
        while ( $copiedRawFileHash{"$raw_type"} ) {
            #print "reset raw type for : $raw_type ($rawFile)\n";
            if ( $raw_type =~ m/(_{0,1}\S*)\_(\d+)/ ) {
                $fileType = $1; 
                $fileIndex = $2; 
                $fileIndex++; 
                $raw_type = "${fileType}_${fileIndex}"; 
            }

            $targetName = "${targetNameRoot}${raw_type}"; 
            #print "new tn:  $targetName\n";

        } 
    
        #   add target name to hash to avoid overwritting files 
        $copiedRawFileHash{$raw_type} = $targetName;
    } else {
        print "\n\tWARN:\tSkipping $rawFile\n";
        $seriesDescription =~ s/(.*)\s+\# series description/$1/;
        printf("\t\tNot a recognized acquisition:                                %s\n", $seriesDescription);
    }
    return $targetName; 
}



#  Method searches for dat files associated with a given P file.
#
#  first input is the P file to be copied (full path)
#  third input is the target name for the pfile. 
#
sub copyDatFiles ($$) 
{

    my($pfile_in, $pfile_out) = @_;

    $scanner_dir = $pfile_in; 
    $scanner_dir =~ s/(.*)\/\S+$/$1/;
    $output_dir = $pfile_out; 
    $output_dir =~ s/(.*)\/\S+$/$1/;

    $fileIndex = 1; 
    if ( $pfile_out =~ m/(\S+\_)(\d+)/ ) {
        $fileIndex = $2; 
    }

    my @result_dat_files = getDatFilenames($pfile_in, $scanner_dir);
    my @output_dat_files;

    my ( $counter, $dat_filename, $dat_path_filename, $dat_output_name, $dat_name_with_suffix, $suffix);

    $suffix = 1;

    # loop over dat files found
    for ($counter = 0; $counter < @result_dat_files; $counter++) {

        # just the filename
        $dat_filename = $result_dat_files[$counter];

        # the filename including the full path
        $dat_path_filename = "${scanner_dir}/${dat_filename}";

        # the target name of the dat file 
        $pfileInName = getFileNameFromPath($pfile_in); 
        $targetNameRoot = "${output_dir}/${pfileInName}";
        if($output_dir =~ m/\/$/){
            $dat_output_name = renameDatFile("${targetNameRoot}", $dat_filename, $fileIndex, $pfile_out);
        } else {
            $dat_output_name = renameDatFile("${targetNameRoot}", $dat_filename, $fileIndex, $pfile_out);
        }

        #   Check to see if we have already written this file
        if (grep {$_ eq $dat_output_name} @output_dat_files) {
            $dat_output_name = "${dat_output_name}_$suffix";
            $suffix++;
        }

        push( @output_dat_files, $dat_output_name );
        $fileNameTmp = getFileNameFromPath($dat_output_name); 
        printf ("\tcopying %-60s to %-60s \n", ${dat_path_filename}, ${fileNameTmp});
        $cmd = "ln -s $dat_path_filename $dat_output_name"; 
        system("$cmd"); 
        #copy($dat_path_filename, $dat_output_name) or die "Copy failed";
        
    }
}

   
#   Method searches for matching dat files within 1 minutes of timestamp 
#
#   first input is the p filename's absolute path
#   second input is the directory path
#
sub getDatFilenames 
{
    my($pfile, $dir) = @_;
    my @dat_files = ();
    my ( $p_number, $time_stamp, $dat_file );


    if( $pfile =~ m/${dir}\/P(\d+)+(\D)+(\d)+(\D)+(\d+)/ ){
        $p_number = $1;
        $time_stamp = $5;
    } else {
        return; # return if the file is not a pfile
    }



    #   now find associated dat files:     
    opendir(DIR, $dir) or die "Couldn't read directory \"$dir\": $!\n";
    my @file_list = readdir(DIR);
    closedir(DIR);

    foreach $dat_file (@file_list) {
        if ( $dat_file =~ m/^P$p_number(\D+)?(\.dat|\.xml)_(\d+)/ ) {

            # Permit +/- 1 on the timestamp
            if( $3 + 2 > $time_stamp && $3 - 2 < $time_stamp ){
                push( @dat_files, $dat_file );
            }
        }
    }

    return @dat_files;
}


#   Method defines new names for output dat files
#
#   first input is the prefix for the file
#   second input is the dat filename w/o directory
#
sub renameDatFile($$$$) 
{
    my ($prefix, $dat_file, $index, $pfile_out ) = @_;
    my $new_name;

    #   check for a pfile suffix (%acqType)
    my $pfile_suffix = ""; 
    if ( $pfile_out =~ m/\S+_(_\S+)_\S+/ ) {
        $pfile_suffix = $1; 
    }

    if($dat_file =~ m/^P(\d+)(\D+)?(\.dat|\.xml)_(\d+)/){   
        if(defined($2)) {
            $new_name = "${prefix}_${pfile_suffix}_${index}${2}${3}";
        } else {
            $new_name = "${prefix}_${pfile_suffix}_${index}${3}";
        }
    }

    return $new_name; 
}

#   
#   Get tmp tnum_dir dir 
#   remove the old one if necessary
#   
sub get_tmp_tnum_dir($)
{
    my ($deleteDir) = @_; 
    
    $cwd = getcwd();
    my $tmp_tnum_dir = File::Spec->rel2abs("${cwd}");
    $tmp_tnum_dir = "${tmp_tnum_dir}/tnum_tmp";  
    if ($deleteDir) { 
        $cmd = "rm -rf  $tmp_tnum_dir" ; 
        if (system( $cmd ) ) {
            print "===============================\n";
            print "Could not delete tmp tnum_dir: \n";
            print "$tmp_tnum_dir \n";
            print "===============================\n\n";
            exit(1);    
        }
        DCM4CHEE::dcm4chee_mkdir_group( $tmp_tnum_dir ); 

        if ( $db_instance =~ m/ms/ ) {
            $cmd = "chgrp -R henrylab $tmp_tnum_dir"; 
        } elsif ( $db_instance =~ m/brain/ ) {
            $cmd = "chgrp -R brain $tmp_tnum_dir"; 
        } else {
            print "no matching db_instance\n"; 
            exit(1); 
        }

        if (system( $cmd ) ) {
            print "===============================\n";
            print "Could not chgrp tmp tnum_dir: \n";
            print "$cmd \n";
            print "===============================\n\n";
            exit(1);    
        }
        $cmd = "chmod g+s  $tmp_tnum_dir"; 
        if (system( $cmd ) ) {
            print "===============================\n";
            print "Could not: \n";
            print "$cmd \n";
            print "===============================\n\n";
            exit(1);    
        }
    }
    return ${tmp_tnum_dir}; 
}


#   
#   Get tmp data transfer dir 
#   remove the old one if necessary
#   
sub get_tmp_transfer_dir($)
{
    my ($deleteDir) = @_; 
    my $cwd = getcwd();
    my $tmp_transfer_dir = File::Spec->rel2abs("${cwd}");
    $tmp_transfer_dir = "${tmp_transfer_dir}/scanner_data_xfr_tmp";  
    if ($deleteDir) {
        $cmd = "rm -rf  $tmp_transfer_dir" ; 
        if (system( $cmd ) ) {
            print "===============================\n";
            print "Could not delete tmp transfer: \n";
            print "$tmp_transfer_dir \n";
            print "===============================\n\n";
            exit(1);    
        }
        DCM4CHEE::dcm4chee_mkdir_group( $tmp_transfer_dir ); 
    }
    return ${tmp_transfer_dir}; 
}


#
#
#
sub update_db_info()
{

    my ($exam, $exam_path, $brain_id, $t_number);

    BRAIN_DB_UTILS::dbutils_get_db_connection();

    print "Output will be written to " . `pwd` . "sql.out \n";
    BRAIN_DB_UTILS::dbutils_set_trace_file("sql.out"); 

    $exam_path = "${tmp_transfer_dir}/${EXAM_NUMBER}";
    my (
        $mrn,
        $first_name,
        $last_name,
        $middle_name,
        $accession_num,
        $formatted_exam_date,
        $scanner_name,
        $field_strength,
        $dob,
        $sex
    ) = BRAIN_DB_UTILS::dbutils_get_exam_info_from_DICOM($exam_path);
    ($brain_id, $t_number) = BRAIN_DB_UTILS::dbutils_lookup_exam($accession_num);

    if ( $t_number eq "null" ) {
        $t_number = "";
    }
    #   if patient name is not in DB (0), then update it now from DICOM attributes.
    #   This also implies that the data processing info/path hasn't yet been uploladed
    #   so do that now as well
    if ( BRAIN_DB_UTILS::dbutils_verify_patient_info($brain_id) == 0 ) {

        #   Update patient name in tpatient table:
        BRAIN_DB_UTILS::dbutils_update_patient_info($brain_id, $last_name, $first_name, $middle_name, $dob, $sex);
    }

    BRAIN_DB_UTILS::dbutils_insert_imaging_event($mrn, $accession_num, $formatted_exam_date, $scanner_name, $field_strength );
    my $study_tag;
    if ( defined $study_tag_in ) {
        $study_tag = $study_tag_in; 
    } elsif ( !defined $study_tag_in ) {
        $study_tag = BRAIN_DB_UTILS::dbutils_get_study_tag_from_path($exam_dir);
    } 
    my $event_ref = BRAIN_DB_UTILS::dbutils_get_event_ref($t_number);
    BRAIN_DB_UTILS::dbutils_update_event($event_ref, $study_tag, $formatted_exam_date);
    BRAIN_DB_UTILS::dbutils_update_processing($t_number);

    BRAIN_DB_UTILS::dbutils_close_db_connection();

    post_update($brain_id, $mrn, $t_number, $accession_num, $formatted_exam_date ); 

}


#
#   Application specific data sync
#
sub post_update($$$$$)
{
    my ($patientID, $mrn, $examID, $accessionNumber, $examDate) = @_; 

    #   this is changing and we will be uploading to champagne
    return; 

    if ( $domain eq "MS" ) {
        #   sync info to MSGenese DB
        my $syncCmd = "ms_sync_with_GenesDB "; 
        $syncCmd   .= " --mrn $mrn "; 
        $syncCmd   .= " --msID $patientID "; 
        $syncCmd   .= " --acc $accessionNumber "; 
        $syncCmd   .= " --mseID $examID ";  
        $syncCmd   .= " --date \'${examDate}\' "; 
        $syncCmd   .= " --pwd CereNelson123 "; 

        if ($DEBUG) {
            print "$syncCmd\n";
        }
        if ( system("$syncCmd") ) {
            print "ERROR, could execute $syncCmd \n";
            exit(1);
        }

    }
    
}

#
#   Application specific data organization 
#
sub post_retrieve($$)
{
    my ($examDir, $examID) = @_; 

    if ( $domain eq "MS" ) {
        $cmd = "ms_make_exam_date_links --examID ${examID} -p $private_pswd"; 
        #print "$cmd\n"; 
        system( "$cmd" ); 
    }
}


#
#   Now, convert raw files to DICOM files and add to exam directory. 
#
sub convert_raw_to_dicom()
{

    print "======================================= \n";
    print "Convert raw to DICOM:                   \n";
    print "======================================= \n";
    if ( $foundRaw == 1 ) {

        my $output_file = BRAIN_DB_UTILS::dbutils_create_output_file("$tmp_transfer_dir", "dcm2raw.log"); 
        my $cmd = "raw2dcm"; 
        $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
        if ( $DEV ) {
            $cmd .= ".dev";
        }
        $cmd .= " --pfile_dir  ${tmp_transfer_dir}  -o ${tmp_transfer_dir}/${EXAM_NUMBER}";
        $cmd .= " >> $output_file " ; 
        
        if ($DEBUG) {
            print "$cmd\n";
        }
        if ( system("$cmd") ) {
            print "ERROR, could not convert raw files to DICOM\n";
            exit(1);
        }
    }
    print "\n";

}


#    
#   Extract the exam:  convert DICOM to idf, DICOM Raw to p-files, .dat, etc. 
#    
sub convert_exam($) 
{

    my ($convertConfigFile) = @_; 
    print "======================================= \n";
    print "Converting exam:  DICOM->idf            \n";
    print "======================================= \n";
    chdir ( ${tmp_transfer_dir} ); 

    my $output_file = BRAIN_DB_UTILS::dbutils_create_output_file("$tmp_transfer_dir", "convert_exam.log"); 

    my @examDirList = DCM4CHEE::dcm4chee_get_exam_numbers_from_directory("${tmp_transfer_dir}/${EXAM_NUMBER}_pacs");

    my $examDirToMove;
    my $numImagesDirs = 1;
    my $imagesDir = "images";
    foreach $examDirToMove ( @examDirList ) {

        chomp $examDirToMove; 

        my $cmd = "convert_exam"; 
        $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
        if( defined $convertConfigFile ) {
            $cmd .= " -c $convertConfigFile ";
        }
        $cmd .= " -e ${examDirToMove} -t ${tnum} >> $output_file 2>&1";

        if ($DEBUG) {
            print "$cmd\n";
        }

        if ( system("$cmd") ) {
            print "ERROR, could not convert exam: $cmd\n";
            exit(1);
        }    

        print "\n";
        print "======================================= \n";
        print "Clean up\n";
        print "======================================= \n";
        if( -d "../$imagesDir" ) {
            $imagesDir = "images_$numImagesDirs";
            $numImagesDirs = $numImagesDirs + 1; 
        } 
        system ("mv images ../$imagesDir");
        system ("mv ${examDirToMove} ../");
        
    }
    chdir  ("../"); 
    system ("rm -rf $tmp_transfer_dir");  
    system ("rm -rf images/Logfile");  

    chdir  ("../"); 

    #
    #   Create target directories
    #

    #   Are we in a b-dir?          
    $cwd = getcwd();
    $path = $cwd;
    $myDir = $path; 
    print "myDir: $myDir \n";
    $myDir =~ s/.*\/(${patientIDPrefix}\S+)$/$1/; 
    print "myDir: $myDir vs ${patientIDPrefix}$bnum\n";
    if ($myDir eq "${patientIDPrefix}${bnum}") { 
        # we are in a correctly named ${patientIDPrefix}-dir
        print("mv tnum_tmp ${examIDPrefix}${tnum}\n"); 
        system ("mv tnum_tmp ${examIDPrefix}${tnum}"); 
    } else { 
        # we are NOT in a b-dir, make it now and move exam into 
        print ("mkdir ${patientIDPrefix}${bnum}\n"); 
        DCM4CHEE::dcm4chee_mkdir_group("${patientIDPrefix}${bnum}"); 
        print ("mv tnum_tmp ${patientIDPrefix}${bnum}/${examIDPrefix}${tnum} \n"); 
        system ("mv tnum_tmp ${patientIDPrefix}${bnum}/${examIDPrefix}${tnum}"); 
    }

    print "\n"; 
}



#
#   Send the identified exam to PACS
#
sub check_pswd()
{
    $cmd = "dcm_echo"; 
    $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );

    if ($DEBUG) {
        $cmd .= " -D "; 
        print "$cmd -p XXXX\n";
    }
    $cmd .= " -p $private_pswd "; 

    my $output_file = BRAIN_DB_UTILS::dbutils_create_output_file("./", "echo.log"); 
    $cmd .= " >> $output_file 2>&1";

    if (system("$cmd")) {
        print "===================================\n";         
        print "ERROR: please check password       \n";  
        print "===================================\n";         
        exit(1);
    }
    system("rm ./echo.log"); 
}


#
#   Send the identified exam to PACS
#
sub archive_exam($)
{
    print "======================================= \n";
    print "Archive DICOM exam in brain PACS        \n";
    print "======================================= \n";

    #   get count of images being transferred, use case insensitive find: 
    #   find -L ./ -iname '*.dcm' | wc
    #   This sould really be done through a storage commitment response
    $number_of_dicom_files_to_archive = get_number_of_dcm_images($exam_dir);
    print "NUMBER OF DICOM IMAGES TO ARCHIVE: $number_of_dicom_files_to_archive\n";

    if (defined $noSnd) {
        return; 
    }

    my ($exam_dir) = @_; 
    $cmd = "dcm_snd"; 
    $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd ); 
    $cmd .= " -g ";     #   just print the command, do not execute it
    $cmd .= " -e $exam_dir "; 

    if ($DEBUG) {
        print "$cmd -p xxxx\n";
    }
    $cmd .= " -p $private_pswd > dcm_snd.log"; 

    #   get the number of series to send (do not count . and .., thus the -2): 
    opendir(DIR, "$exam_dir") or die "Couldn't read directory $examDir $!\n";
    my @seriesDirs = readdir(DIR);
    closedir(DIR);
    $numTasks = 0; 
    foreach my $dd (@seriesDirs) {
        if ( $dd=~ m/\w+/ ) {
            $numTasks++; 
        }
    }

    #$numTasks = @seriesDirs - 2; 

    $DCM_SND_LOG_ROOT = "dcm_snd_log"; 
    for ($i = 1; $i <= $numTasks; $i++) {
        $logFile = "${DCM_SND_LOG_ROOT}.${i}";
        unlink("$logFile");
    }

    my @qsub_thread_array;
    $qsub_thread_array[0] = async {
        system("$cmd"); 
    }
    $qsub_thread_array[0];

    $numImagesSent = 0; 
    $qsub_thread_array[1] = async {

        $numImagesSent = DCM4CHEE::dcm4chee_monitor_grid_job( $DCM_SND_LOG_ROOT, $numTasks, 0 ); 

        print "number of images archvied: $numImagesSent\n"; 
        if ($numImagesSent != $number_of_dicom_files_to_archive ) {
            print "===================================\n";         
            print "ERROR:  Did not archive all files  \n";         
            print "        $numImagesSent / $number_of_dicom_files_to_archive\n";         
            print "===================================\n";         
            exit(1); 
        }
    }
    $qsub_thread_array[1];

    $qsub_thread_array[1]->join();
    $qsub_thread_array[0]->join();

}


#
#   Create the job file with the correct perms for the daemon on braino to 
#   pick up. 
#
sub writeDaemonJobFile($$@)
{

    my($jobFileRoot, $jobDir, @commands) = @_; 

    $jobFile = "${jobDir}/${jobFileRoot}"; 
    chomp $jobFile; 

    $cmd = "touch ${jobFile}"; 
    $status = system ("$cmd"); 

    ($login, $pass, $uid, $gid) = getpwnam("dcm4chee"); 
    if ( $status == 0 ) {
        $status = chmod(0600, "$jobFile"); 
        #  status is the number of files changed.  Should be 1: 
        if ( $status == 1) {
            $status = 0; 
        }
    }

    open(DAEMON_JOB, ">$jobFile") || die $!;
    foreach $c ( @commands ) {
        printf (DAEMON_JOB "%s\n", $c);  
    }
    close(DAEMON_JOB); 

    if ( $status == 0 ) {
       $status = chown($uid, $gid, "$jobFile"); 
        #  status is the number of files changed.  Should be 1: 
        if ( $status == 1) {
            $status = 0; 
        }
    }
   
    if ( $status == 0 ) {
        $status = rename ("$jobFile", "${jobFile}.in"); 
        #  status is the number of files changed.  Should be 1: 
        if ( $status == 1) {
            $status = 0; 
        }
    }

    if ( $status ) {
        print "===================================\n";         
        print "ERROR:  Could not archive $exam_dir\n";         
        print "===================================\n";         
        exit(1); 
    } 

}


#
#   Check for creation of status file, and parse value of 
#   status from the file. 
#
#   Returns 0 if the job completes successfully. 
#
sub monitorDaemonJobsSCP($$$) 
{

    my($jobFileRoot, $jobDir, $numFilesToArchive) = @_;  

    #   wait for job to complete: 
    $jobFile = "${jobDir}/${jobFileRoot}"; 
    $count = 0; 
    $timeOut = 60 * 20;     #  20 minutes
    $checkForStatus = 1; 
    while ( $checkForStatus && ($count < $timeOut) ) {
        sleep(10); 
        $count += 10; 
        if ( -f "${jobFile}.status" ) {
            STDOUT->printflush( ". " );
            #   Get the status from the file
            open(STATUS_FILE, "${jobFile}.status") || die $!;
            @contents = <STATUS_FILE>; 
            close(STATUS_FILE); 

            #   assume it failed, unless detecting a status = 0 in file: 
            #   don't break loop until a status value is found
            $status = 1; 
            $progress = -1;
            foreach $line ( @contents) {
                if ( $line =~ m/status\:(\d+)/ )  {
                    $status = $1; 
                    $checkForStatus = 0; 
                    last; 
                } 
                if ( $line =~ m/.*\>\> (\d+)\:C-STORE-RSP.*/ )  {
                    $progress = $1;
                }

            }
            if ( $progress >= 0 ) {
                chomp $numFilesToArchive;
                $percent  = 100 * $progress/$numFilesToArchive;
                printf ("progress: %4.2f \%\n", $percent );
            }

        }
    }

    if ( $count >= $timeOut ) {
        print "ERROR: archiving timed out ($jobFileRoot)\n"; 
        exit(1); 
    }

    if ( $status ) {
        print "ERROR: archiving exam to PACS ($jobFileRoot)\n"; 
        exit(1); 
    }

    print "\n"; 
    return status; 

}


#
#   Check for creation of status file, and parse value of 
#   status from the file. 
#       input: 
#           numCommands is equal to the number of series that should be retrieved. 
#
#   Returns 0 if the job completes successfully. 
#
sub monitorDaemonJobsQR($$$) 
{

    my( $jobFileRoot, $jobDir, $numCommands ) = @_;  

    #   wait for job to complete: 
    $jobFile = "${jobDir}/${jobFileRoot}"; 
    $timeCount = 0; 
    $timeOut = 60 * 20;     #  20 minutes
    $checkForStatus = 1; 
    while ( $checkForStatus && ($timeCount < $timeOut) ) {
        $statusCount = 0; 
        sleep(10); 
        $timeCount += 10; 
        if ( -f "${jobFile}.status" ) {
            #   Get the status from the file
            open(STATUS_FILE, "${jobFile}.status") || die $!;
            @contents = <STATUS_FILE>; 
            close(STATUS_FILE); 

            #   assume it failed, unless detecting a status = 0 in file: 
            #   don't break loop until a status value is found
            $status = -1; 
            foreach $line ( @contents) {
                if ( $line =~ m/status\:(\d+)/ )  {
                    $statusCount++; 
                    $status = $1; 
                    #   if any of the commands returns a non-zero status, then exit: 
                    if ( $status > 0 ) {
                        last; 
                    }
                } 
            }
            $percentSeriesRecvd = 100 * $statusCount / $numCommands; 
            printf ( "series received(%d/$numCommands): %d\% \n", $statusCount, $percentSeriesRecvd ); 
            if ( ($statusCount == $numCommands) || ($status > 0)  ) {
                $checkForStatus = 0; 
            }
            if ( $statusCount != $numCommands ) {
                $status = 1; 
            }
        } else {
            print "."; 
        }
    }

    if ( $timeCount >= $timeOut ) {
        print "ERROR: archiving timed out ($jobFileRoot)\n"; 
        exit(1); 
    }

    if ( $status ) {
        print "ERROR: archiving exam to PACS ($jobFileRoot)\n"; 
        exit(1); 
    }

    print "\n"; 
    return status; 

}


#
#   Retrieve the identified exam from PACS
#
sub retrieve_exam($)
{

    print "======================================= \n";
    print "Retrieve DICOM exam from $domain PACS   \n";
    print "======================================= \n";

    my ($tnum, $exam_dir) = @_; 
    $exam_dir = "${exam_dir}_pacs"; 
    DCM4CHEE::dcm4chee_mkdir_group("$exam_dir");

    $cmd = "dcm_qr"; 
    $cmd = BRAIN_DB_UTILS::dbutils_get_instance_cmd_name( $cmd );
    $cmd .= " -g  ";     #   just print the dcm_qr command, do not run it
    $cmd .= " --gsd";     #   just print the dcm_qr command, do not run it

    if ($DEBUG) {
        $cmd .= " -D "; 
    }
    $cmd .= " -e $exam_dir -t $tnum "; 

    if ($DEBUG) {
        print "$cmd xxxx\n";
    }
    $cmd .= " -p $private_pswd > dcm_qr.log"; 

    my $accession_number = BRAIN_DB_UTILS::dbutils_get_accession_from_tnum($tnum);
    my @qrOutput = DCM4CHEE::dcm4chee_qr_series($accession_number, $exam_dir, "*", "", "", 1);
    my $numTasks = @qrOutput/2;   

    $DCM_QR_LOG_ROOT = "dcm_qr_log";
    for ($i = 1; $i <= $numTasks; $i++) {
        $logFile = "${DCM_QR_LOG_ROOT}.${i}";
        unlink("$logFile");
    }

    my @qsub_thread_array;
    $qsub_thread_array[0] = async {
        system("$cmd");
    }
    $qsub_thread_array[0];

    $numImagesSent = 0;
    $qsub_thread_array[1] = async {

        $numImagesRecvd = DCM4CHEE::dcm4chee_monitor_grid_job( $DCM_QR_LOG_ROOT, $numTasks, 1 );

        if ( !defined($noSnd)) {
            if ($numImagesRecvd != $number_of_dicom_files_to_archive ) {
                print "===================================\n";
                print "ERROR:  Did not retrieve all files  \n";
                print "        ${numImagesRecvd}/${number_of_dicom_files_to_archive}\n";
                print "===================================\n";
                exit(1);
            }
        }

    }
    $qsub_thread_array[1];

    $qsub_thread_array[1]->join();
    $qsub_thread_array[0]->join();

    $hiddenDir = "${exam_dir}/.qrtmp";
    #print "GSD CLEANUP!!! $exam_dir $hiddenDir \n"; 
    DCM4CHEE::dcm4chee_qr_cleanup("$exam_dir", "$hiddenDir", "${patientIDPrefix}${bnum}", "${examIDPrefix}${tnum}");

    #   clean up the log files   
    @rm_glob = glob("${exam_dir}/../qss*");
    foreach $f ( @rm_glob ) {
        unlink ("$f");
    }
    @rm_glob = glob("${exam_dir}/../${DCM_QR_LOG_ROOT}*");
    foreach $f ( @rm_glob ) {
        unlink ("$f");
    }


}




sub checkForBDir() 
{

    #   Are we in a b-dir?          
    $path = $start_dir;
    $myDir = $path; 
    $myDir =~ s/.*\/(${patientIDPrefix}\S+)$/$1/; 
    if ($myDir ne "${patientIDPrefix}${bnum}") { 

        # we are NOT in a b-dir, make it now and move exam into 
        chdir ("$start_dir");
        if( !(-d "${patientIDPrefix}${bnum}") ) {
            DCM4CHEE::dcm4chee_mkdir_group("${patientIDPrefix}${bnum}"); 
        }

        $cmd = "mv tnum_tmp ${patientIDPrefix}${bnum}/tnum_tmp"; 
        system ("$cmd"); 

        chdir ("${patientIDPrefix}${bnum}");

        #   reset these: 
        $tmp_tnum_dir = get_tmp_tnum_dir(0); 
        chdir("$tmp_tnum_dir"); 

        $tmp_transfer_dir = get_tmp_transfer_dir(0);
        chdir("$tmp_transfer_dir"); 
        
    }
    if ($DEBUG) {
        print "pwd: ". `pwd` . "\n";
    }
    $cwd = getcwd();
}


#   find number of dicom images in specified dir 
#   (based on  dcm or DCM file extension). 
sub get_number_of_dcm_images($)
{
    my ($dir) = @_;
    $number_of_dicom_files = `/usr/bin/find -L $dir -iname '*.dcm' | wc`; 
    $number_of_dicom_files =~  s/\s+(\d+)\s+.*/$1/;
    return $number_of_dicom_files; 
}



#   Sort rawData by it's time stamp from earliest to most recent
sub sortRawFilesByTime(@) 
{
    my (@rawData) = @_;
    my %timeSortedRawData;
    foreach $f (@rawData) {
        if ( $f =~ m/.*P(\d+)\..*_(\d+)/ ) {
            $pNum = $1;
            $timeStamp = $2;
            #   Sort by date then by pnumber. Dynamic C13 data might have timestamp collisions.
            $timeSortedRawData{$f} = "${timeStamp}.${pNum}";
        } else {
            print "Error, can not find time stamp for pfile: $f\n";
            exit(1);
        }
    }

    my @rawDataSorted;
    foreach $key ( sort { $timeSortedRawData{$a} <=> $timeSortedRawData{$b} } keys %timeSortedRawData ) {
        push(@rawDataSorted, $key);
    }

    return @rawDataSorted;
}


###############################################################################
#
#   POD Usage docs
#
###############################################################################

=head1 NAME_BRAIN

    get_scanner_data

=head1 NAME_MS

    ms_get_scanner_data

=head1 SYNOPSIS_BRAIN

    get_scanner_data -a accession | -t tnum [ --no_snd ] --study studyTag [ --meta ] 
                     [ --recognized_raw ] [ -Dh ]

        -a                  accession_num   Accession number for exams to retrieve
        -t                  tnum            t-number for exam to retrieve
        --no_snd                            Do not send to group PACS
        --study             studyTag        Set the study tag for this exam.  Must be a tag registered in the braino DB
        --meta                              Only upload meta data (imagining attributes), but don't send do PACS
        --recognized_raw                    Print recognized raw file series descriptions to be archived (do not archive). 
        -c                  convert_config  Specify a configuration file for converting to UCSF idf format.
        -D                                  Turn on debugging 
        -h                                  Print help message.

=head1 SYNOPSIS_MS

    ms_get_scanner_data -a accession | -t mseID [ --no_snd ] [ --study studyTag ] [ --meta ]
                        [ --recognized_raw ] [ -Dh ]

        -a                  accession_num   Accession number for exam to retrieve
        -t                  mseID           MS Exam ID for exam to retrieve
        --no_snd                            Do not send to group PACS
        --study             studyTag        Set the study tag for this exam.  Must be a tag registered in the mspacman DB
        --meta                              Only upload meta data (imagining attributes), but don't send do PACS
        --recognized_raw                    Print recognized raw file series descriptions to be archived (do not archvie). 
        -D                                  Turn on debugging 
        -h                                  Print help message.

=head1 DESCRIPTION

Program to retrieve scanner data.   Given an exam identifier, locates and makes a local copy of the DICOM images 
and p-files inside a patientID/examID directory.  Will create patientID and examID directories if they don't already 
exist.  

On completion the retrieved data will be deidentified. 


=head1 EXAMPLES_BRAIN

    get_scanner_data -t 1234 
    get_scanner_data -a 01234567

=head1 EXAMPLES_MS

    ms_get_scanner_data -t 1234 
    ms_get_scanner_data -a 01234567


=cut

