#!/usr/bin/env perl
#####################
## V2, taskx.v2.pl
## Based on taskx.pl, 04/25/2019 modified by juyanmei@genomics.cn
####################
use strict;
use warnings;
use Getopt::Std;
our ($opt_n,$opt_j,$opt_u,$opt_h);
getopts('n:j:u:h');
&usage if $opt_h;
#print "ID\tusername\tName        \tstate\t        applyMem,cpuNum\tuseMem      \tmaxMem      \tcpuTime\tnode\n------------------------------------------------------------------------------------------------------------------------\n";
print "ID  username  Name  state  applyMem,cpuNum  useMem  maxMem  cpuTime  node\n----------------------------------------------------------------------\n";

&jobinfo($opt_j) if ($opt_j);
&user_jobinfo($opt_u) if ((!$opt_u && !$opt_j && !$opt_n) || $opt_u);
&host_jobinfo($opt_n) if ($opt_n);

sub usage {
    print STDERR "\t-n : hostname/node\n";
    print STDERR "\t-j : job number\n";
    print STDERR "\t-u : user name\n";
    print STDERR "\t-h : help\n";
    print STDERR "Contact: juyanmei\@genomics.cn\n";
    exit(1);
}

sub jobstate {
    my $jobid = $_[0];
    my $qstat = `qstat -j $jobid`;
    my $owner = $1 if ($qstat =~ /owner:.+?(\w+)\n/);
    open I, "qstat -u $owner |";
    <I>;
    <I>;
    while (<I>) {
        s/^\s+//g;
        # add tasknode
        my ($taskid, $taskstate, $tasknodeF) = (split /\s+/)[0, 4, 7];
        if ($jobid =~ /$taskid/) {
            my $tasknode = 0;
            if ($tasknodeF =~ /.+@(.+?)\./) {
            $tasknode = $1;
            }
            return($taskstate, $tasknode);
        } 
    }
}

sub jobinfo {
    my $jobid = $_[0];
    my $taskstate = $_[1];
    my $tasknode = $_[2];
    # add tasknode
    ($taskstate, $tasknode) = &jobstate($jobid) unless $taskstate; 
    my $qstat = `qstat -j $jobid`;
    my $owner = $1 if ($qstat =~ /owner:.+?(\w+)\n/);
    my $taskname = $1 if ($qstat =~ /job_name:.+?(\w+.+?)\n/);
    my ($needmem, $usemem, $maxmem, $cpu_num, $cpu_time) = (0, 0, 0, 0, 0); 
    $needmem = $1 if ($qstat =~ /virtual_free=(\d+.?\w)/);
    $usemem = $1 if ($qstat =~ /vmem=(\w+.\w+)/);
    $maxmem = $1 if ($qstat =~ /maxvmem=(\w+.\w+)/);
    $cpu_num = $1 if ($qstat =~ /num_proc=(\d+)/);
    $cpu_time = $1 if ($qstat =~ /cpu=(.+?),/);
    #print "$jobid\t$owner\t$taskname\t$taskstate\t$needmem,$cpu_num\t$usemem  \t$maxmem  \t$cpu_time\t$tasknode\n";
    print "$jobid  $owner  $taskname  $taskstate  $needmem,$cpu_num  $usemem  $maxmem  $cpu_time  $tasknode\n";
}

sub user_jobinfo {
    my $username = $_[0];
    $username = `whoami` unless $username;
    chomp $username;
    open I, "qstat -u $username |";
    <I>;
    <I>;
    while (<I>) {
        s/^\s+//g;
        my ($taskid, $taskstate, $tasknodeF) = (split /\s+/)[0, 4, 7];
        my $tasknode = 0;
        if ($tasknodeF =~ /.+@(.+?)\./) {
        $tasknode = $1;
        }
        &jobinfo($taskid, $taskstate, $tasknode);
    }
}

sub host_jobinfo {
    my $host = $_[0];
    my $index = 0;
    open I, "qhost -j |";
    while (<I>) {
        if (/^$host/) {
            $index = 1;
            while (<I>) {
                 if (/^\s+/) {
                     my ($taskid, $taskstate) = (split /\s+/)[1, 5];
                     &jobinfo($taskid, $taskstate, $host);
                 } else {
                     last; 
                 }
            } 
        }
        if ($index == 1) {
            last;
        }
    }
}

