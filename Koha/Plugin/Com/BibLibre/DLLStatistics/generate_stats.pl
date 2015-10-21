#!/usr/bin/perl

use Modern::Perl;

use Pod::Usage;
use Getopt::Long;
use Data::Dumper;

use C4::Context;

use lib C4::Context->config("pluginsdir");
use Koha::Plugin::Com::BibLibre::DLLStatistics;

my ( $help, $verbose, $execute, $debug, @titles, $year, $email );
GetOptions(
    'h|help'    => \$help,
    'v|verbose' => \$verbose,
    'e|execute' => \$execute,
    'd|debug'   => \$debug,
    't|title:s' => \@titles,
    'y|year:s'  => \$year,
    'email:s'   => \$email,
) || pod2usage(1);

if ($help) {
    pod2usage(1);
}

if ( not $debug and not $execute ) {
    pod2usage( q|At least debug or execute should be given| );
}

$verbose //= $debug;

my $plugin = Koha::Plugin::Com::BibLibre::DLLStatistics->new();
$plugin->run({
    verbose => $verbose,
    execute => $execute,
    debug   => $debug,
    titles  => \@titles,
    year    => $year,
    email   => $email,
})
