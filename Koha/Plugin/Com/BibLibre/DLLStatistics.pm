package Koha::Plugin::Com::BibLibre::DLLStatistics;

use Modern::Perl;

use base qw( Koha::Plugins::Base );

use Cwd qw(abs_path);
use POSIX qw(strftime);
use YAML;
use Data::Dumper;
use C4::Output;
use C4::Scheduler;
use MIME::Lite;

our $VERSION = 0.02;

our $metadata = {
    name   => 'DLL Statistics',
    author => 'BibLibre',
    description => 'DLL Statistics',
    date_authored   => '2015-10-20',
    date_updated    => '2015-10-20',
    minimum_version => '3.1800000',
    maximum_version => undef,
    version         => $VERSION,
};

sub new {
    my ( $class, $args ) = @_;

    $args->{'metadata'} = $metadata;
    $args->{'metadata'}->{'class'} = $class;

    my $self = $class->SUPER::new($args);

    return $self;
}

sub configure {
    my ($self) = @_;

    my $cgi = $self->{'cgi'};

    unless ($cgi->param('save')) {
        my $template = $self->get_template({ file => 'configure.tt' });

        $template->param(
            conf => $self->retrieve_data('conf'),
        );

        output_html_with_http_headers $cgi, undef, $template->output;
    }
    else {
        $self->store_data({
            conf => $cgi->param('conf'),
        });
        $self->go_home();
    }
}

sub tool {
    my ($self) = @_;

    my $cgi = $self->{cgi};
    my $class = $self->{class};

    unless ( $cgi->param('run') ) {
        my $template = $self->get_template({ file => 'tool.tt' });

        output_html_with_http_headers $cgi, undef, $template->output;
    }
    else {
        my $time = $cgi->param('time');
        my $year = $cgi->param('year');
        my $email = $cgi->param('email');
        my $command = abs_path($self->mbf_path('generate_stats.pl'))
            . " -e -y $year --email $email";
        add_at_job($time, $command);
        $self->go_home();
    }
}

sub run {
    my ($self, $params) = @_;

    my $verbose = $params->{verbose};
    my $execute = $params->{execute};
    my $debug = $params->{debug};
    my @titles = @{ $params->{titles} };
    my $year = $params->{year};
    my $email = $params->{email};

    $verbose //= $debug;

    if ( $execute ) {
        $self->init();
    }

    my $blocks = $self->get_blocks($year);
    for my $block (@$blocks) {
        $self->build_queries( $block );
        if ( @titles ) {
            $self->filter_block( $block, \@titles );
        }
    }

    if ( @titles ) {
        # Filter first level
        $blocks = [ map { my $b = $_; @{ $b->{blocks} } ? $b : () } @$blocks ];
    }

    my $output = '';
    for my $block (@$blocks) {
        $output .= $self->process_block($block, {
            verbose => $verbose,
            execute_queries => $execute
        });
    }

    if ($execute) {
        $self->end();
    }

    if ($email) {
        my $msg = MIME::Lite->new(
            From => C4::Context->preference('KohaAdminEmailAddress'),
            To => $email,
            Subject => "Statistiques DLL $year",
            Type => 'TEXT',
            Data => $output,
        );
        $msg->send;
    } else {
        say $output;
    }
}

sub create_view {
    my ($self, $table) = @_;

    my $dbh = C4::Context->dbh;

    my $c = $self->get_conf();
    my $conf = $c->{$table . '_fields'};
    my @fields;
    if ($conf) {
        foreach my $name (keys %$conf) {
            my $expr = $conf->{$name};
            push @fields, "$expr as $name";
        }
    }

    $self->drop_view($table);

    my $view_name = $table . '_view';
    my $view_sql = "CREATE VIEW $view_name AS SELECT *";
    if (@fields) {
        $view_sql .= ', ' . join(', ', @fields);
    }
    $view_sql .= " FROM $table";
    $dbh->do($view_sql);
}

sub drop_view {
    my ($self, $table) = @_;

    my $dbh = C4::Context->dbh;
    my $view_name = $table . '_view';
    $dbh->do("DROP VIEW IF EXISTS $view_name");
}

sub init {
    my ($self) = @_;

    my $dbh = C4::Context->dbh;
    $dbh->{RaiseError} = 1;

    $self->create_view('items');
    $self->create_view('deleteditems');
    $self->create_view('biblioitems');
}

sub end {
    my ($self) = @_;

    my $dbh = C4::Context->dbh;

    $self->drop_view('items');
    $self->drop_view('deleteditems');
    $self->drop_view('biblioitems');
}

sub get_conf {
    my ($self) = @_;

    return $self->{conf} if defined $self->{conf};

    my $conf = $self->retrieve_data('conf');
    die 'No configuration defined in the DLLStatistics syspref'
        unless $conf;
    $self->{conf} = YAML::Load( $conf . "\n" );
    return $self->{conf};
}

sub get_separator {
    my ($self) = @_;

    my $c = $self->get_conf();
    return $c->{separator} || q|,|;
}

sub get_original_conditions {
    my ( $self, $type ) = @_;
    my @original_conditions;
    my $conf = $self->get_conf();
    if ( $type eq 'biblio' ) {
        my $barcodes_to_exclude = $conf->{barcodes_to_exclude};
        my $branches_to_exclude = $conf->{branches_to_exclude};
        my $depouillement = $conf->{depouillement};
        my $separator = $self->get_separator();
        if ($barcodes_to_exclude) {
            for my $barcode ( split $separator, $barcodes_to_exclude ) {
                push @original_conditions, qq|barcode!=$barcode|;
            }
        }
        if ($branches_to_exclude) {
            for my $branch ( split $separator, $branches_to_exclude ) {
                push @original_conditions, qq|homebranch!=$branch|;
            }
        }
        if ($depouillement) {
            push @original_conditions, @{ $self->negate($depouillement) };
        }
    } elsif ( $type eq 'borrower' ) {
        # Nothing here at the moment
    }
    return @original_conditions;
}

sub build_sql_query {
    my ($self, $params) = @_;
    my $query = $params->{based_query};
    my $binop = $params->{binop} // 'AND';
    my $conditions = $params->{conditions} || [];
    my $groupby = $params->{groupby};
    my $separator = $self->get_separator();

    my ( $where_query, $where_args ) = $self->build_where(
        {
            conditions => $conditions,
            binop      => 'AND',
            separator  => $separator,
        }
    );

    my @args;
    if ($where_query) {
        $query .= " AND $where_query";
        push @args, @$where_args;
    }

    $query .= " GROUP BY $groupby" if $groupby;

    return ( $query, \@args );
}

sub build_where {
    my ($self, $params) = @_;
    my $conditions = $params->{conditions};
    my $binop     = $params->{binop}     || q|AND|;
    my $separator = $self->get_separator();

    my ( @where, @args );
    for my $condition (@$conditions) {
        if (ref $condition eq 'ARRAY') {
            my ($w, $a) = $self->build_where({conditions => $condition, binop => $binop eq 'AND' ? 'OR' : 'AND'});
            push @where, "($w)";
            push @args, @$a;
        }
        elsif ( $condition =~ m#
                ^
                (?<field>(\w|\.)*)
                ((?<operator>(!|<|>|=)+))
                (?<value>.*)
                $#x ) {
            my ( $field, $operator, $value ) = ( $+{field}, $+{operator}, $+{value} );

            my $not = ( $operator eq '!=' ) ? 'NOT' : '';
            my @sub_where;
            for my $v ( split '\|', $value ) {
                push @sub_where, $v =~ m|%|
                  ? ($not ? "$not " : '' ) . "$field LIKE ?"
                  : $v =~ m|^NULL$|
                    ? "$field IS " . ( $not ? "$not " : '' ) . "NULL"
                    : ($operator eq '=' or $operator eq '!=')
                      ? "$not $field <=> ?"
                      : "$field $operator ?";
                push @args, $v unless $v =~ m|^NULL$| ;
            }
            push @where, '( ' . join( " OR ", @sub_where ) . ' )';
        }
        else {
            warn
"WARNING: the condition '$condition' does not match the expected rule format!";
        }
    }
    return ( join( " $binop ", @where ), \@args );
}

sub build_output {
    my ($self, $params) = @_;
    my $data = $params->{data};
    my ( $output, $total ) = ( q||, 0 );
    for my $k ( @$data ) {
        for my $k1 ( sort keys %$k ) {
            if ( $k1 eq 'total' ) {
                $total += $k->{total};
            } else {
                $output .= $k->{$k1} . ': ' . $k->{total} . "\n";
            }
        }
    }
    $output .= "Total: $total";
    return $output;
}

# $block will be modified by this routine!
sub build_queries {
    my ( $self, $block, $query_params, $level ) = @_;
    my @queries;
    $query_params //= {};
    my %query_params = %$query_params;
    $level ||= 0;
    if ( $block->{based_query} ) {
        $query_params{based_query} = $block->{based_query};
    }
    if ( $block->{groupby} ) {
        $query_params{groupby} = $block->{groupby};
    }
    if ( $block->{conditions} ) {
        $query_params{conditions} = $block->{conditions};
    }
    if ( $block->{additional_conditions} ) {
        $query_params{conditions} = [
            $query_params{conditions}
            ? (
                @{ $query_params{conditions} },
                @{ $block->{additional_conditions} }
              )
            : @{ $block->{additional_conditions} }
        ];
    }
    $query_params{title} = $block->{title};
    $block->{level} = $level;
    $level++;
    if ( not exists $block->{queries} and not exists $block->{blocks} ) {
        ( $block->{sql_query}, $block->{sql_query_params} ) = $self->build_sql_query( \%query_params );
    }
    elsif ( exists $block->{blocks} ) {
        for my $b ( @{ $block->{blocks} } ) {
            $self->build_queries( $b, \%query_params, $level );
        }
    }
    elsif ( exists $block->{queries} ) {
        for my $query ( @{ $block->{queries} } ) {
            $self->build_queries( $query, \%query_params, $level );
        }
    }
    else {
        die "The queries structure is badly defined";
    }
}

# $block will be modified by this routine!
sub filter_block {
    my ( $self, $block, $titles ) = @_;
    if ( exists $block->{blocks} ) {
        for my $block ( @{ $block->{blocks} } ) {
            $self->filter_block( $block, $titles);
        }
        $block->{blocks} = [
            map {
                my $b = $_;
                if (   ( $b->{queries} and not @{ $b->{queries} } )
                    or ( $b->{blocks} and not @{ $b->{blocks} } )
                    or ( not $b->{queries} and not $b->{blocks} ) )
                {
                    ();
                }
                else {
                    $b;
                }
            } @{ $block->{blocks} }
        ];
    }
    elsif ( exists $block->{queries} ) {
        my @queries = ();
        for my $query ( @{ $block->{queries} } ) {
            if ( grep /^$query->{title}$/, @$titles ) {
                push @queries, $query;
            }
        }
        $block->{queries} = \@queries;
    }
    else {
        die "The queries structure is badly defined";
    }
}

sub process_block {
    my ( $self, $block, $params ) = @_;
    my $verbose = $params->{verbose};
    my $execute = $params->{execute_queries};

    my $dbh = C4::Context->dbh;
    my $output = '';

    if ( $verbose ) {
        my $indent = "=" x ($block->{level} + 1);
        $output .= $indent . ' ' . $block->{title} . ' ' . $indent . "\n";
    }
    if ( $block->{blocks} ) {
        for my $b ( @{ $block->{blocks} } ) {
            $output .= $self->process_block( $b, { verbose => $verbose, execute_queries => $execute } );
        }
    }
    elsif ( $block->{queries} ) {
        for my $query ( @{ $block->{queries} } ) {
            my $indent = "=" x ($query->{level} + 1);
            $output .= $indent . ' ' . $query->{title};
            if ($query->{label}) {
                $output .= ' - ' . $query->{label};
            }
            $output .= ' ' . $indent . "\n";
            if ( $execute ) {
                my ( $data, $result ) ;
                eval {
                    die "Query not allowed" unless $query->{sql_query} =~ m|^\s*select|i;
                    my @sql_query_params = ( $query->{sql_query_params} and @{ $query->{sql_query_params} } ) ? @{ $query->{sql_query_params} } : ();
                    $data = $dbh->selectall_arrayref( $query->{sql_query}, { Slice => {} }, @sql_query_params );
                    $result = 1;
                };

                if ( $@ ) {
                    $output .= "=== Error on executing the following query ===\n";
                    $output .= $query->{sql_query} . "\n";
                    $output .= Dumper($query->{sql_query_params}) . "\n";
                    $output .= $@ . "\n";
                    $output .= "==============================================\n";
                } elsif ( $result ) {
                    if ( $verbose ) {
                        $output .= $query->{sql_query} . "\n";
                        $output .= Dumper($query->{sql_query_params}) . "\n";
                    }
                    $output .= $self->build_output( { data => $data } ) . "\n";
                }
            } elsif ( $verbose ) {
                $output .= $query->{sql_query} . "\n";
                $output .= Dumper($query->{sql_query_params}) . "\n";
            }
        }
    }

    return $output;
}

sub negate_r {
    my ($self, $depth, @conditions) = @_;

    my @newconditions;
    foreach my $condition (@conditions) {
        my $newcondition;

        if (not ref $condition) {
            if ($condition =~ /^(?<field>(\w|\.)*)((?<operator>(!|<|>|=)+))(?<value>.*)$/) {
                my ($field, $operator, $value) = ($+{field}, $+{operator}, $+{value});
                my $newoperator =
                    $operator eq '=' ? '!=' :
                    $operator eq '!=' ? '=' :
                    $operator eq '>=' ? '<' :
                    $operator eq '>' ? '<=' :
                    $operator eq '<=' ? '>' :
                    $operator eq '<' ? '>=' : '';

                # If value contains pipes, we need to split here in order to
                # negate the query correctly
                if ($value =~ /\|/) {
                    $newcondition = [];
                    foreach my $v (split /\|/, $value) {
                        push @$newcondition, $field . $newoperator . $v;
                    }
                    if ($depth % 2) {
                        # We are already at "OR depth", add another level of
                        # depth so that conditions are correctly OR'ed
                        $newcondition = [ $newcondition ];
                    }
                } else {
                    $newcondition = $field . $newoperator . $value;
                }
            } else {
                warn "Bad format";
            }
        } else {
            $newcondition = [ $self->negate_r($depth + 1, @$condition) ];
        }

        push @newconditions, $newcondition;
    }

    return @newconditions;
}

sub negate {
    my ($self, $condition) = @_;

    # Make all arrays one level deeper, so that AND becomes OR and vice versa
    return [[ $self->negate_r(0, @$condition) ]];
}

sub get_blocks {
    my ($self, $date_of_this_year) = @_;

    my $conf            = $self->get_conf();
    my $separator       = $self->get_separator();
    my @livres_imprimes = @{ $conf->{livres_imprimes} };
    my @libre_access    = @{ $conf->{libre_access} };
    my @publications_en_serie_imprimees = @{ $conf->{publications_en_serie_imprimees} };
    my @microformes     = @{ $conf->{microformes} };
    my @documents_cartographiques = @{ $conf->{documents_cartographiques} };
    my @musique_imprimee = @{ $conf->{musique_imprimee} };
    my @documents_graphiques = @{ $conf->{documents_graphiques} };
    my @autres_documents = @{ $conf->{autres_documents} };
    $date_of_this_year ||= $conf->{year};

    die "No year has been defined. You must defined one (cronjob param or syspref)" unless $date_of_this_year;

    my @documents_sonores_musiques = @{ $conf->{documents_sonores_musiques} };
    my @documents_sonores_livres_enregistres = @{ $conf->{documents_sonores_livres_enregistres} };
    my @documents_video = @{ $conf->{documents_video} };
    my @documents_multimedia = @{ $conf->{documents_multimedia} };
    my @livres_numeriques = @{ $conf->{livres_numeriques} };
    my @dons = @{ $conf->{dons} };

    my @date_before_1811 = (q|publicationyear<1811|);
    my @date_between_1811_1914 =
      ( q|publicationyear>=1811|, q|publicationyear<=1914| );

    my @audience_enfants = @{ $conf->{audience_enfants} };
    my @audience_adultes = @{ $conf->{audience_adultes} };

    my $today = strftime "%Y-%m-%d", localtime;

    my @enfants = @{ $conf->{enfants} };
    my @adultes = @{ $conf->{adultes} };
    my @seniors = @{ $conf->{seniors} };
    my @date_of_birth_enfants =
      ( q|dateofbirth>=| . ( $date_of_this_year - 14 ) . q|-01-01| );
    my @date_of_birth_adultes = (
        q|dateofbirth<=| . ( $date_of_this_year - 15 ) . q|-12-31|,
        q|dateofbirth>=| . ( $date_of_this_year - 64 ) . q|-12-31|
    );
    my @date_of_birth_seniors =
      ( q|dateofbirth<=| . ( $date_of_this_year - 65 ) . q|-01-01| );

    my @residents_dans_la_commune = @{ $conf->{residents_dans_la_commune} };
    my @collectivites = @{ $conf->{collectivites} };

    my $peb_branchcode = $conf->{peb_branchcode};
    my $peb_categorycode = $conf->{peb_categorycode};

    my @depouillement = @{ $conf->{depouillement} };

    my $total_items_query = q|
        SELECT homebranch, COUNT(DISTINCT(biblionumber)) as total
        FROM items_view items
        WHERE 1
    |;
    my $total_biblio_join_items_query = q|
        SELECT homebranch, COUNT(DISTINCT(biblio.biblionumber)) as total
        FROM biblio
        JOIN items_view items ON items.biblionumber = biblio.biblionumber
        JOIN biblioitems_view biblioitems ON items.biblionumber = biblioitems.biblionumber
        WHERE 1
    |;
    my $total_item_join_biblioitems_query = q|
        SELECT homebranch, COUNT(*) as total
        FROM items_view
        JOIN biblioitems_view ON items_view.biblioitemnumber = biblioitems_view.biblioitemnumber
        WHERE 1
    |;

    my $total_deleteditems_join_biblioitems_query = q|
        SELECT homebranch, COUNT(*) as total
        FROM deleteditems_view deleteditems
        JOIN biblioitems_view biblioitems ON deleteditems.biblioitemnumber = biblioitems.biblioitemnumber
        WHERE 1
    |;

    my $total_subscriptionid_from_subscription_join_items_join_biblioitems_query =
      q|
        SELECT subscription.branchcode, COUNT(DISTINCT(subscription.subscriptionid)) as total
        FROM subscription
        JOIN items_view items ON subscription.biblionumber = items.biblionumber
        JOIN biblioitems_view biblioitems ON subscription.biblionumber = biblioitems.biblionumber
        WHERE 1
    |;

    my $total_biblionumber_from_subscription_join_items_join_biblioitems_query = q|
        SELECT subscription.branchcode, COUNT(DISTINCT(subscription.biblionumber)) as total
        FROM subscription
        JOIN items_view items ON subscription.biblionumber = items.biblionumber
        JOIN biblioitems_view biblioitems ON subscription.biblionumber = biblioitems.biblionumber
        WHERE 1
    |;

    my $total_borrowers_query = q|
        SELECT branchcode, count(*) as total
        FROM borrowers
        WHERE 1
    |;

    my $total_borrowers_join_statistics_query = q|
        SELECT branchcode, COUNT(DISTINCT(borrowers.borrowernumber)) as total
        FROM borrowers
        JOIN statistics ON borrowers.borrowernumber = statistics.borrowernumber
        WHERE 1
    |;

    my $total_borrowers_by_categorycode_query = q|
        SELECT borrowers.categorycode, count(*) as total
        FROM borrowers
        JOIN categories ON (borrowers.categorycode = categories.categorycode)
        WHERE 1
    |;

    my $total_statistics_join_borrowers_items_biblioitems_query = q|
        SELECT branchcode, COUNT(*) as total
        FROM statistics
        JOIN borrowers ON borrowers.borrowernumber = statistics.borrowernumber
        JOIN items_view items ON items.itemnumber = statistics.itemnumber
        JOIN biblioitems_view biblioitems ON items.biblionumber = biblioitems.biblionumber
        WHERE 1
    |;

    my $total_old_reserves_query = q|
        SELECT branchcode, COUNT(*) as total
        FROM old_reserves
        WHERE 1
    |;

    my $total_deleteditems_query = q|
        SELECT holdingbranch, COUNT(*) as total
        FROM deleteditems
        WHERE 1
    |;

    my @original_conditions_for_biblio = $self->get_original_conditions('biblio');

    my $blocks = [

        # C2 - Catalogue
        {
            title      => 'C - Accès et installations',
            conditions => \@original_conditions_for_biblio,

            blocks => [
                {
                    title           => 'C2 - Catalogue',
                    default_groupby => 'homebranch',
                    queries         => [
                        {
                            title       => 'C203',
                            based_query => $total_items_query,
                            groupby     => 'homebranch',
                        },
                        {
                            title       => 'C204',
                            based_query => $total_biblio_join_items_query,
                            groupby     => 'homebranch',
                            additional_conditions => [qq|datecreated=$date_of_this_year%|],
                        },
                    ]
                }
            ]
        },
        {
            title  => 'D - Collections',
            blocks => [
                {
                    title       => 'D1 - Imprimés',
                    groupby     => 'homebranch',
                    based_query => $total_item_join_biblioitems_query,
                    conditions  => \@original_conditions_for_biblio,
                    blocks      => [
                        {
                            title                 => 'Adultes',
                            additional_conditions => [@audience_adultes],
                            blocks                => [
                                {
                                    title                 => 'Livres imprimés',
                                    additional_conditions => [@livres_imprimes],
                                    queries               => [
                                        {
                                            title => 'D101',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte - Fonds",
                                        },
                                        {
                                            title => 'D102',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte - Acquistions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D103',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte - Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D104',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte - Fonds en libre acces",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D105',
                                            label => "Nombre de titres 'Livres imprimés' adulte - Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D106',
                                            label => "Nombre de titres 'Livres imprimés' adulte - Fonds Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D107',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte publiés avant 1811 - Fonds",
                                            additional_conditions => [@date_before_1811],
                                        },
                                        {
                                            title                 => 'D108',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte publiés avant 1811 - Acquisitions",
                                            additional_conditions => [
                                                @date_before_1811,
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D109',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte publiés entre 1811 et 1914 - Fonds",
                                            additional_conditions => [@date_between_1811_1914],
                                        },
                                        {
                                            title                 => 'D110',
                                            label => "Nombre d'exemplaires 'Livres imprimés' adulte publiés entre 1811 et 1914 - Acquistions",
                                            additional_conditions => [
                                                @date_between_1811_1914,
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ]
                                },
                                {
                                    title => "Publications en série imprimées",
                                    additional_conditions => [@publications_en_serie_imprimees],
                                    queries => [
                                        {
                                            title => 'D111',
                                            label => "Nombre d'exemplaires 'Publications en série' adulte - Fonds",
                                        },
                                        {
                                            title => 'D112',
                                            label => "Nombre d'exemplaires 'Publications en série' adulte - Acquistions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D113',
                                            label => "Nombre d'exemplaires 'Publications en série' adulte - Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D114',
                                            label => "Nombre d'exemplaires 'Publications en série' adulte - Fonds en libre acces",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D115',
                                            label => "Nombre de titres 'Publications en série' adulte - Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D141',
                                            label => "Nombre de titres 'Publications en série' adulte - Fonds Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ],
                                },
                            ]
                        },
                        {
                            title                 => 'Enfants',
                            additional_conditions => [@audience_enfants],
                            blocks                => [
                                {
                                    title                 => 'Livres imprimés',
                                    additional_conditions => [@livres_imprimes],
                                    queries               => [
                                        {
                                            title => 'D116',
                                            label => "Nombre d'exemplaires 'Livres imprimés' enfant - Fonds",
                                        },
                                        {
                                            title                 => 'D117',
                                            label => "Nombre d'exemplaires 'Livres imprimés' enfant - Acquistions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D118',
                                            label => "Nombre d'exemplaires 'Livres imprimés' enfant - Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D119',
                                            label => "Nombre d'exemplaires 'Livres imprimés' enfant - Fonds en libre acces",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D120',
                                            label => "Nombre de titres 'Livres imprimés' enfant - Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D121',
                                            label => "Nombre de titres 'Livres imprimés' enfant - Fonds Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ]
                                },
                                {
                                    title => "Publications en série imprimées",
                                    additional_conditions => [@publications_en_serie_imprimees],
                                    queries => [
                                        {
                                            title => 'D122',
                                            label => "Nombre d'exemplaires 'Publications en série' enfant - Fonds",
                                        },
                                        {
                                            title => 'D123',
                                            label => "Nombre d'exemplaires 'Publications en série' enfant - Acquistions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D124',
                                            label => "Nombre d'exemplaires 'Publications en série' enfant - Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D125',
                                            label => "Nombre d'exemplaires 'Publications en série' enfant - Fonds en libre acces",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D126',
                                            label => "Nombre de titres 'Publications en série' enfant - Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D127',
                                            label => "Nombre de titres 'Publications en série' enfant - Fonds Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ],
                                },
                            ]
                        },
                        {
                            title  => 'Total',
                            blocks => [
                                {
                                    title                 => 'Livres imprimés',
                                    additional_conditions => [@livres_imprimes],
                                    queries               => [
                                        {
                                            title => 'D128',
                                            label => "Total Exemplaires 'Livres imprimés'",
                                        },
                                        {
                                            title                 => 'D129',
                                            label => "Total Exemplaires 'Livres imprimés' Acquistions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D130',
                                            label => "Total Exemplaires 'Livres imprimés' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D131',
                                            label => "Total Exemplaires 'Livres imprimés' Dons",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|,
                                                @dons,
                                            ],
                                        },
                                        {
                                            title => 'D132',
                                            label => "Total Exemplaires 'Livres imprimés' Fonds libre accès",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D133',
                                            label => "Total Titres 'Livres imprimés' Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D134',
                                            label => "Total Titres 'Livres imprimés' Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ]
                                },
                                {
                                    title => "Publications en série imprimées",
                                    additional_conditions => [@publications_en_serie_imprimees],
                                    queries => [
                                        {
                                            title => 'D135',
                                            label => "Total Exemplaires 'Publications en série' Fonds",
                                        },
                                        {
                                            title                 => 'D136',
                                            label => "Total Exemplaires 'Publications en série' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D137',
                                            label => "Total Exemplaires 'Publications en série' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                        {
                                            title => 'D138',
                                            label => "Total Exemplaires 'Publications en série' en libre Accès",
                                            additional_conditions => [@libre_access],
                                        },
                                        {
                                            title => 'D139',
                                            label => "Total Titres 'Publications en série' Fonds",
                                            based_query => $total_biblio_join_items_query,
                                        },
                                        {
                                            title => 'D140',
                                            label => "Total Titres 'Publications en série' Acquisitions",
                                            based_query => $total_biblio_join_items_query,
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                    ],
                                },
                            ]
                        }
                    ]
                },
                {
                    title      => 'D2 - Publications en série en cours',
                    groupby    => 'branchcode',
                    conditions => \@original_conditions_for_biblio,
                    blocks     => [
                        {
                            title                  => 'Adultes',
                            additional_conditions => [@audience_adultes],
                            queries                => [
                                {
                                    title => 'D201',
                                    label => "Nombre d'abonnements Adultes",
                                    based_query => $total_subscriptionid_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [
                                        qq|enddate>=$date_of_this_year-01-01|,
                                    ]
                                },
                                {
                                    title => 'D202',
                                    label => "Nombre de titres 'Publications en série' Adultes",
                                    based_query => $total_biblionumber_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [
                                        qq|enddate>=$date_of_this_year-01-01|,
                                    ]
                                },
                                {
                                    title => 'D208',
                                    based_query => $total_biblionumber_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [qq|enddate>=$date_of_this_year-01-01|]
                                }
                            ]
                        },
                        {
                            title => 'Enfants',
                            additional_conditions => [@audience_enfants],
                            queries => [
                                {
                                    title => 'D203',
                                    label => "Nombre d'abonnements Enfants",
                                    based_query => $total_subscriptionid_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [
                                        qq|enddate>=$date_of_this_year-01-01|,
                                    ]
                                },
                                {
                                    title => 'D204',
                                    label => "Nombre de titres 'Publications en série' Enfants",
                                    based_query => $total_biblionumber_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [
                                        qq|enddate>=$date_of_this_year-01-01|,
                                    ]
                                },
                                {
                                    title => 'D210',
                                    based_query => $total_biblionumber_from_subscription_join_items_join_biblioitems_query,
                                    additional_conditions => [
                                        qq|enddate>=$date_of_this_year-01-01|,
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    title      => 'D3 - Autres documents',
                    groupby    => 'homebranch',
                    conditions => \@original_conditions_for_biblio,
                    blocks     => [
                        {
                            title => 'Microformes',
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@microformes],
                            queries => [
                                {
                                    title => 'D305',
                                    label => "Nombre d'exemplaires 'Microformes' Fonds",
                                },
                                {
                                    title => 'D306',
                                    label => "Nombre d'exemplaires 'Microformes' Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|,
                                    ]
                                },
                            ]
                        },
                        {
                            title => 'Documents cartographiques',
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@documents_cartographiques],
                            queries => [
                                {
                                    title => 'D307',
                                    label => "Nombre d'exemplaires 'Documents cartographiques' Fonds",
                                },
                                {
                                    title => 'D308',
                                    label => "Nombre d'exemplaires 'Documents cartographiques' Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|,
                                    ]
                                },
                            ]
                        },
                        {
                            title => 'Musique imprimée',
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@musique_imprimee],
                            queries => [
                                {
                                    title => 'D309',
                                    label => "Nombre d'exemplaires 'Musique imprimée' Fonds",
                                },
                                {
                                    title => 'D310',
                                    label => "Nombre d'exemplaires 'Musique imprimée' Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|,
                                    ]
                                },
                                {
                                    title => 'D311',
                                    label => "Nombre d'exemplaires 'Musique imprimée' en libre accès",
                                    additional_conditions => [@libre_access]
                                },
                                {
                                    title => 'D312',
                                    label => "Nombre de titres 'Musique imprimée'",
                                    based_query => $total_biblio_join_items_query,
                                },
                                {
                                    title => 'D313',
                                    label => "Nombre de titres 'Musique imprimée' Acquisitions",
                                    based_query => $total_biblio_join_items_query,
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ]
                                },
                            ]
                        },
                        {
                            title => 'Documents graphiques',
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@documents_graphiques],
                            queries => [
                                {
                                    title => 'D314',
                                    label => "Nombre d'exemplaires 'Documents graphiques' Fonds",
                                },
                                {
                                    title => 'D315',
                                    label => "Nombre d'exemplaires 'Documents graphiques' Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ]
                                },
                            ]
                        },
                        {
                            title => 'Autres documents',
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@autres_documents],
                            queries => [
                                {
                                    title => 'D318',
                                    label => "Nombre d'exemplaires 'Autres Documents' Fonds",
                                },
                                {
                                    title => 'D319',
                                    label => "Nombre d'exemplaires 'Autres Documents' Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ]
                                },
                                {
                                    title => 'D320',
                                    label => "Nombre d'exemplaires 'Autres documents' Eliminations",
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                },
                            ]
                        },
                    ]
                },
                {
                    title  => 'D4 - Documents audiovisuels',
                    based_query => $total_item_join_biblioitems_query,
                    groupby => 'homebranch',
                    blocks => [
                        {
                            title                 => "Documents sonores: musique",
                            additional_conditions => [@documents_sonores_musiques],
                            queries               => [
                                {
                                    title => 'D401',
                                    label => "Documents sonores: Musique Fonds",
                                },
                                {
                                    title => 'D402',
                                    label => "Documents sonores: Musique Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ],
                                },
                            ]
                        },
                        {
                            title => "Documents sonores: livres enregistrés",
                            additional_conditions => [@documents_sonores_livres_enregistres],
                            queries => [
                                {
                                    title => 'D405',
                                    label => "Documents sonores : livres enregistrés Fonds",
                                },
                                {
                                    title => 'D406',
                                    label => "Documents sonores : livres enregistrés Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ],
                                },
                            ]
                        },
                        {
                            title                 => "Total documents sonores",
                            additional_conditions => [[
                                [@documents_sonores_musiques],
                                [@documents_sonores_livres_enregistres],
                            ]],
                            queries => [
                                {
                                    title => 'D409',
                                    label => "Total Documents sonores Fonds",
                                },
                                {
                                    title => 'D410',
                                    label => "Total Documents sonores Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ],
                                },
                            ]
                        },
                        {
                            title                 => "Documents vidéo",
                            additional_conditions => [@documents_video],
                            queries               => [
                                {
                                    title => 'D411',
                                    label => "Total Documents vidéos Fonds",
                                },
                                {
                                    title => 'D412',
                                    label => "Total Documents vidéos Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ],
                                },
                            ]
                        },
                        {
                            title                 => "Éliminations",
                            queries => [
                                {
                                    title => 'D415',
                                    label => "Total Documents sonores et vidéos Eliminitions",
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                    additional_conditions => [[
                                        [@documents_sonores_musiques],
                                        [@documents_sonores_livres_enregistres],
                                        [@documents_video],
                                    ]],
                                },
                                {
                                    title => 'D416',
                                    label => "Documents sonores: Musique Eliminations",
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                    additional_conditions => [@documents_sonores_musiques],
                                },
                                {
                                    title => 'D417',
                                    label => "Documents sonores : livres enregistrés Eliminations",
                                    additional_conditions => [@documents_sonores_livres_enregistres],
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                },
                                {
                                    title => 'D418',
                                    label => "Total Documents sonores Eliminations",
                                    additional_conditions => [[
                                        [@documents_sonores_musiques],
                                        [@documents_sonores_livres_enregistres],
                                    ]],
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                },
                                {
                                    title => 'D419',
                                    label => "Total Documents vidéos Eliminitions",
                                    additional_conditions => [@documents_video],
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                },
                            ]
                        },
                        {
                            title => "Documents Audiovisuels Adultes",
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@audience_adultes],
                            groupby => 'homebranch',
                            blocks => [
                                {
                                    title                 => "Documents sonores: musique",
                                    additional_conditions => [@documents_sonores_musiques],
                                    queries               => [
                                        {
                                            title => 'D420',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Fonds",
                                        },
                                        {
                                            title => 'D421',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D422',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title => "Documents sonores: livres enregistrés",
                                    additional_conditions => [@documents_sonores_livres_enregistres],
                                    queries => [
                                        {
                                            title => 'D423',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Fonds",
                                        },
                                        {
                                            title => 'D424',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D425',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title                 => "Total documents sonores",
                                    additional_conditions => [[
                                        [@documents_sonores_musiques],
                                        [@documents_sonores_livres_enregistres],
                                    ]],
                                    queries => [
                                        {
                                            title => 'D426',
                                            label => "Total des 'Documents sonores' Adultes Fonds",
                                        },
                                        {
                                            title => 'D427',
                                            label => "Total des 'Documents sonores' Adultes Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D428',
                                            label => "Total des 'Documents sonores' Adultes Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title                 => "Documents vidéo",
                                    additional_conditions => [@documents_video],
                                    queries               => [
                                        {
                                            title => 'D429',
                                            label => "Nombre d'exemplaires 'Documents Vidéos' Fonds",
                                        },
                                        {
                                            title => 'D430',
                                            label => "Nombre d'exemplaires 'Documents Vidéos : Musique' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D431',
                                            label => "Nombre d'exemplaires 'Documents Vidéos' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                            ],
                        },
                        {
                            title => "Documents Audiovisuels Enfants",
                            based_query => $total_item_join_biblioitems_query,
                            additional_conditions => [@audience_enfants],
                            groupby => 'homebranch',
                            blocks => [
                                {
                                    title                 => "Documents sonores: musique",
                                    additional_conditions => [@documents_sonores_musiques],
                                    queries               => [
                                        {
                                            title => 'D432',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Fonds",
                                        },
                                        {
                                            title => 'D433',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D434',
                                            label => "Nombre d'exemplaires 'Documents sonores : Musique' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title => "Documents sonores: livres enregistrés",
                                    additional_conditions => [@documents_sonores_livres_enregistres],
                                    queries => [
                                        {
                                            title => 'D435',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Fonds",
                                        },
                                        {
                                            title => 'D436',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D437',
                                            label => "Nombre d'exemplaires 'Documents sonores : livres enregistrés' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title                 => "Total documents sonores",
                                    additional_conditions => [[
                                        [@documents_sonores_musiques],
                                        [@documents_sonores_livres_enregistres],
                                    ]],
                                    queries => [
                                        {
                                            title => 'D438',
                                            label => "Total des 'Documents sonores' Enfants Fonds",
                                        },
                                        {
                                            title => 'D439',
                                            label => "Total des 'Documents sonores' Enfants Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D440',
                                            label => "Total des 'Documents sonores' Enfants Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                                {
                                    title                 => "Documents vidéo",
                                    additional_conditions => [@documents_video],
                                    queries               => [
                                        {
                                            title => 'D441',
                                            label => "Nombre d'exemplaires 'Documents Vidéos' Fonds",
                                        },
                                        {
                                            title => 'D442',
                                            label => "Nombre d'exemplaires 'Documents Vidéos : Musique' Acquisitions",
                                            additional_conditions => [
                                                qq|dateaccessioned=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'D443',
                                            label => "Nombre d'exemplaires 'Documents Vidéos' Eliminations",
                                            based_query => $total_deleteditems_join_biblioitems_query,
                                        },
                                    ]
                                },
                            ],
                        }
                    ]
                },
                {
                    title       => 'D5 - Documents numériques',
                    based_query => $total_item_join_biblioitems_query,
                    groupby     => 'homebranch',
                    blocks => [
                        {
                            title => "Autres documents numériques",
                            additional_conditions => [[
                                [@documents_sonores_musiques],
                                [@documents_sonores_livres_enregistres],
                                [@documents_video],
                            ]],
                            queries => [
                                {
                                    title => 'D506',
                                    label => "Total Documents sonores et vidéo Fonds",
                                },
                                {
                                    title => 'D507',
                                    label => "Total Documents sonores et vidéo Acquisitions",
                                    additional_conditions => [
                                        qq|dateaccessioned>=$date_of_this_year-01-01|
                                    ],
                                }
                            ]
                        },
                        {
                            title => "Documents multimédia sur support",
                            additional_conditions => [@documents_multimedia],
                            queries => [
                                {
                                    title => 'D517',
                                    label => "Total documents multimédia sur support(cdrom+logiciel) enfants+adultes Fonds",
                                },
                                {
                                    title                 => 'D518',
                                    label => "Total documents multimédia sur support(cdrom+logiciel) enfants+adultes Acquistions",
                                    additional_conditions => [
                                        qq|dateaccessioned=$date_of_this_year%|
                                    ],
                                },
                                {
                                    title => 'D519',
                                    label => "Total documents multimédia sur support(cdrom+logiciel) enfants+adultes Eliminations",
                                    based_query => $total_deleteditems_join_biblioitems_query,
                                },
                            ]
                        }
                    ]
                },
            ]
        },
        {
            title  => 'E - Usages et usagers de la bibliothèque',
            blocks => [
                {
                    title   => 'E1 - Usagers',
                    groupby => 'branchcode',
                    additional_conditions => [
                        "dateexpiry>=$today"
                    ],
                    blocks  => [
                        {
                            title       => 'Particuliers',
                            based_query => $total_borrowers_query,
                            blocks => [
                                {
                                    title => 'Enfants',
                                    additional_conditions => [
                                        @enfants,
                                        @date_of_birth_enfants,
                                    ],
                                    queries => [
                                        {
                                            title => 'E104',
                                            label => "Particuliers Enfants(0-14 ans) Hommes Inscrits actifs",
                                            additional_conditions => [q|sex=M|],
                                        },
                                        {
                                            title                 => 'E105',
                                            label => "Particuliers Enfants(0-14 ans) Hommes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E106',
                                            label => "Particuliers Enfants(0-14 ans) Hommes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E107',
                                            label => "Particuliers Enfants(0-14 ans) Femmes Inscrits actifs",
                                            additional_conditions => [q|sex=F|],
                                        },
                                        {
                                            title                 => 'E108',
                                            label => "Particuliers Enfants(0-14 ans) Femmes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E109',
                                            label => "Particuliers Enfants(0-14 ans) Femmes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E110',
                                            label => "Total Particuliers Enfants(0-14 ans) Inscrits actifs",
                                        },
                                        {
                                            title => 'E111',
                                            label => "Total Particuliers Enfants(0-14 ans) Nouveaux inscrits",
                                            additional_conditions => [
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E112',
                                            label => "Total Particuliers Enfants(0-14 ans) Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew},
                                            ],
                                        },

                                    ]
                                },
                                {
                                    title => 'Adultes (de 15 à 64 ans)',
                                    additional_conditions => [
                                        @adultes,
                                        @date_of_birth_adultes,
                                    ],
                                    queries => [
                                        {
                                            title => 'E113',
                                            label => "Particuliers Adultes(15-64 ans) Hommes Inscrits actifs",
                                            additional_conditions => [q|sex=M|],
                                        },
                                        {
                                            title                 => 'E114',
                                            label => "Particuliers Adultes(15-64 ans) Hommes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E115',
                                            label => "Particuliers Adultes(15-64 ans) Hommes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E116',
                                            label => "Particuliers Adultes(15-64 ans) Femmes Inscrits actifs",
                                            additional_conditions => [q|sex=F|],
                                        },
                                        {
                                            title                 => 'E117',
                                            label => "Particuliers Adultes(15-64 ans) Femmes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E118',
                                            label => "Particuliers Adultes(15-64 ans) Femmes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E119',
                                            label => "Total Particuliers Adultes(15-64 ans) Inscrits actifs",
                                        },
                                        {
                                            title => 'E120',
                                            label => "Total Particuliers Adultes(15-64 ans) Nouveaux inscrits",
                                            additional_conditions => [
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E121',
                                            label => "Total Particuliers Adultes(15-64 ans) Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                    ]
                                },
                                {
                                    title => 'Adultes (de 65 ans et plus)',
                                    additional_conditions => [
                                        @seniors,
                                        @date_of_birth_seniors,
                                    ],
                                    queries => [
                                        {
                                            title => 'E122',
                                            label => "Particuliers Adultes(65 ans et plus) Hommes Inscrits actifs",
                                            additional_conditions => [q|sex=M|],
                                        },
                                        {
                                            title => 'E123',
                                            label => "Particuliers Adultes(65 ans et plus) Hommes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E124',
                                            label => "Particuliers Adultes(65 ans et plus) Hommes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E125',
                                            label => "Particuliers Adultes(65 ans et plus) Femmes Inscrits actifs",
                                            additional_conditions =>
                                              [q|sex=F|],
                                        },
                                        {
                                            title                 => 'E126',
                                            label => "Particuliers Adultes(65 ans et plus) Femmes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E127',
                                            label => "Particuliers Adultes(65 ans et plus) Femmes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E128',
                                            label => "Total Particuliers Adultes(65 ans et plus) Inscrits actifs",
                                        },
                                        {
                                            title => 'E129',
                                            label => "Total Particuliers Adultes(65 ans et plus) Nouveaux inscrits",
                                            additional_conditions => [ qq|dateenrolled=$date_of_this_year%| ],
                                        },
                                        {
                                            title => 'E130',
                                            label => "Total Particuliers Adultes(65 ans et plus) Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                    ]
                                },
                                {
                                    title   => 'Total Adultes',
                                    additional_conditions => [[
                                        [@adultes, @date_of_birth_adultes],
                                        [@seniors, @date_of_birth_seniors],
                                    ]],
                                    queries => [
                                        {
                                            title => 'E131',
                                            label => "Total Particuliers Adultes Hommes Inscrits actifs",
                                            additional_conditions => [
                                                q|sex=M|,
                                            ]
                                        },
                                        {
                                            title => 'E132',
                                            label => "Total Particuliers Adultes Hommes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ]
                                        },
                                        {
                                            title => 'E133',
                                            label => "Total Particuliers Adultes Hommes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=M|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E134',
                                            label => "Total Particuliers Adultes Femmes Inscrits actifs",
                                            additional_conditions => [
                                                q|sex=F|,
                                            ]
                                        },
                                        {
                                            title => 'E135',
                                            label => "Total Particuliers Adultes Femmes Nouveaux inscrits",
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ]
                                        },
                                        {
                                            title => 'E136',
                                            label => "Total Particuliers Adultes Femmes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                q|sex=F|,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title => 'E137',
                                            label => "Total Particuliers Adultes Inscrits actifs",
                                        },
                                        {
                                            title => 'E138',
                                            label => "Total Particuliers Adultes Nouveaux inscrits",
                                            additional_conditions => [
                                                qq|dateenrolled=$date_of_this_year%|
                                            ]
                                        },
                                        {
                                            title => 'E139',
                                            label => "Total Particuliers Adultes Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                    ],
                                },
                                {
                                    title   => 'Total',
                                    additional_conditions => [[
                                        [@enfants, @date_of_birth_enfants],
                                        [@adultes, @date_of_birth_adultes],
                                        [@seniors, @date_of_birth_seniors],
                                    ]],
                                    queries => [
                                        {
                                            title => 'E101',
                                            label => "Total Particuliers (enfants et adultes) Inscrits actifs",
                                        },
                                        {
                                            title                 => 'E102',
                                            label => "Total Particuliers (enfants et adultes) Nouveaux inscrits",
                                            additional_conditions => [
                                                qq|dateenrolled=$date_of_this_year%|
                                            ]
                                        },
                                        {
                                            title => 'E103',
                                            label => "Total Particuliers (enfants et adultes) Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                        {
                                            title                 => 'E140',
                                            label => "Total Particuliers (enfants et adultes) de la commune ou du réseau Inscrits actifs",
                                            additional_conditions => [
                                                @residents_dans_la_commune
                                            ],
                                        },
                                        {
                                            title                 => 'E141',
                                            label => "Total Particuliers (enfants et adultes) de la commune ou du réseau Nouveaux Inscrits",
                                            additional_conditions => [
                                                @residents_dans_la_commune,
                                                qq|dateenrolled=$date_of_this_year%|
                                            ],
                                        },
                                        {
                                            title => 'E142',
                                            label => "Total Particuliers (enfants et adultes) de la commune ou du réseau Emprunteurs actifs",
                                            based_query => $total_borrowers_join_statistics_query,
                                            additional_conditions => [
                                                @residents_dans_la_commune,
                                                qq|datetime=$date_of_this_year%|,
                                                q{statistics.type=issue|renew}
                                            ],
                                        },
                                    ]
                                },
                            ]
                        },
                        {
                            title       => 'Collectivités',
                            based_query => $total_borrowers_query,
                            additional_conditions => [@collectivites],
                            queries               => [
                                {
                                    title => 'E143',
                                    label => "Collectivités Nouveaux Inscrits",
                                    additional_conditions =>
                                      [qq|dateenrolled=$date_of_this_year%|],
                                },
                                {
                                    title => 'E144',
                                    label => "Collectivités Emprunteurs actifs",
                                    based_query => $total_borrowers_join_statistics_query,
                                    additional_conditions => [
                                        qq|datetime=$date_of_this_year%|,
                                        q/statistics.type=issue|renew/
                                    ],
                                }
                            ]
                        }

                    ]
                },
                {
                    title   => 'E2 - Prêts',
                    groupby     => 'branchcode',
                    based_query => $total_statistics_join_borrowers_items_biblioitems_query,
                    additional_conditions => [
                        'type=issue|renew',
                        "datetime=$date_of_this_year%",
                    ],
                    blocks  => [
                        {
                            title => 'Usagers',
                            blocks => [
                                {
                                    title  => 'Livres',
                                    additional_conditions => [@livres_imprimes],
                                    queries => [
                                        {
                                            title => 'E201',
                                            label => "Total Prêts Livres Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E202',
                                            label => "Total Prêts Livres Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E203',
                                            label => "Total Prêts Livres",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Publications en série imprimées',
                                    additional_conditions => [@publications_en_serie_imprimees],
                                    queries => [
                                        {
                                            title => 'E205',
                                            label => "Total Prêts Publications en série Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E206',
                                            label => "Total Prêts Publications en série Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E207',
                                            label => "Total Prêts Publications en série",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents sonores: musiques',
                                    additional_conditions => [@documents_sonores_musiques],
                                    queries => [
                                        {
                                            title => 'E209',
                                            label => "Total Prêts Documents sonores : Musique Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E210',
                                            label => "Total Prêts Documents sonores : Musique Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E211',
                                            label => "Total Prêts Documents sonores : Musique",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents sonores: livres enregistrés',
                                    additional_conditions => [@documents_sonores_livres_enregistres],
                                    queries => [
                                        {
                                            title => 'E213',
                                            label => "Total Prêts Documents sonores : Livres Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E214',
                                            label => "Total Prêts Documents sonores : Livres Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E215',
                                            label => "Total Prêts Documents sonores : Livres",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents vidéo',
                                    additional_conditions => [@documents_video],
                                    queries => [
                                        {
                                            title => 'E217',
                                            label => "Total Prêts Documents Vidéos Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E218',
                                            label => "Total Prêts Documents Vidéos Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E219',
                                            label => "Total Prêts Documents Vidéos",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Autres documents',
                                    additional_conditions => [@autres_documents],
                                    queries => [
                                        {
                                            title => 'E221',
                                            label => "Total Prêts Autres Documents Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E222',
                                            label => "Total Prêts Autres Documents Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E223',
                                            label => "Total Prêts Autres Documents Livres",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Livres numériques avec support',
                                    additional_conditions => [@livres_numeriques],
                                    queries => [
                                        {
                                            title => 'E225',
                                            label => "Total Prêts Livres numériques avec support Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E226',
                                            label => "Total Prêts Livres numériques avec support Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E227',
                                            label => "Total Prêts Livres numériques avec support Livres",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                                {
                                    title  => 'Total',
                                    queries => [
                                        {
                                            title => 'E237',
                                            label => "Total Prêts Adultes",
                                            additional_conditions => [[
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                        {
                                            title => 'E238',
                                            label => "Total Prêts Enfants",
                                            additional_conditions => [
                                                @enfants,
                                                @date_of_birth_enfants,
                                            ],
                                        },
                                        {
                                            title => 'E239',
                                            label => "Total Prêts (Adultes et Enfants)",
                                            additional_conditions => [[
                                                [@enfants, @date_of_birth_enfants],
                                                [@adultes, @date_of_birth_adultes],
                                                [@seniors, @date_of_birth_seniors],
                                            ]],
                                        },
                                    ],
                                },
                            ],
                        },

                        {
                            title       => 'Collectivités',
                            additional_conditions => [@collectivites],
                            blocks                => [
                                {
                                    title  => 'Livres',
                                    additional_conditions => [@livres_imprimes],
                                    queries => [
                                        {
                                            title => 'E204',
                                            label => "Total Prêts Livres (aux collectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Publications en série imprimées',
                                    additional_conditions => [@publications_en_serie_imprimees],
                                    queries => [
                                        {
                                            title => 'E208',
                                            label => "Total Prêts Publications en série (aux collectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents sonores: musiques',
                                    additional_conditions => [@documents_sonores_musiques],
                                    queries => [
                                        {
                                            title => 'E212',
                                            label => "Total Prêts  Documents sonores : Musique(aux collectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents sonores: livres enregistrés',
                                    additional_conditions => [@documents_sonores_livres_enregistres],
                                    queries => [
                                        {
                                            title => 'E216',
                                            label => "Total Prêts  Documents sonores : Livres(aux collectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Documents vidéo',
                                    additional_conditions => [@documents_video],
                                    queries => [
                                        {
                                            title => 'E220',
                                            label => "Total Prêts  Documents vidéo (aux collectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Autres documents',
                                    additional_conditions => [@autres_documents],
                                    queries => [
                                        {
                                            title => 'E224',
                                            label => "Total Prêts Autres Documents (aux colectivités)",
                                        },
                                    ],
                                },
                                {
                                    title  => 'Total',
                                    queries => [
                                        {
                                            title => 'E240',
                                            label => "Total Prêts aux collectivités",
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
                {
                    title => "E3 - Autres Types d'usage",
                    queries => [
                        {
                            title => 'E301',
                            label => "Nombre de consultations sur place",
                            based_query => $total_statistics_join_borrowers_items_biblioitems_query,
                            groupby => 'homebranch',
                            additional_conditions => [
                                'type=localuse',
                            ]
                        },
                        {
                            title => 'E302',
                            label => "Nombre de réservations",
                            based_query => $total_old_reserves_query,
                            groupby => 'branchcode',
                            additional_conditions => [
                                "timestamp=$date_of_this_year%",
                            ]
                        },
                        {
                            title => 'E306',
                            label => "Prêts entre Bibliothèques - Documents reçus",
                            based_query => $total_deleteditems_query,
                            groupby => 'holdingbranch',
                            additional_conditions => [
                                "timestamp=$date_of_this_year%",
                                "homebranch=$peb_branchcode",
                            ]
                        },
                        {
                            title => 'E307',
                            label => "Prêts entre Bibliothèques - Documents fournis",
                            based_query => $total_statistics_join_borrowers_items_biblioitems_query,
                            groupby => 'homebranch',
                            additional_conditions => [
                                "categorycode=$peb_categorycode",
                                'type=issue|renew',
                                "datetime=$date_of_this_year%",
                            ]
                        },
                    ],
                },
            ]
        },
        {
            title => 'Z - Potential issues',
            blocks => [
                {
                    title => 'Z1 - Notices',
                    queries => [
                        {
                            title => 'Z101',
                            label => "Notices de dépouillement avec un mauvais label (463)",
                            based_query => q{
                                SELECT COUNT(*) AS total FROM biblioitems_view
                                WHERE ExtractValue(marcxml, '//datafield[@tag="463"]/subfield') != ''
                                  AND leader67 != 'aa'
                            },
                        },
                        {
                            title => 'Z102',
                            label => "Notices de dépouillement avec un mauvais label",
                            based_query => q{
                                SELECT COUNT(DISTINCT(biblioitems.biblionumber)) AS total FROM biblioitems_view biblioitems
                                JOIN items_view items ON (items.biblionumber = biblioitems.biblionumber)
                                WHERE 1
                            },
                            additional_conditions => [
                                'leader67!=aa',
                                @depouillement,
                            ],
                        },
                    ],
                },
                {
                    title => 'Z2 - Exemplaires',
                    queries => [
                        {
                            title => 'Z201',
                            label => "Exemplaires ayant un public différent de Adulte ou Enfant",
                            based_query => $total_item_join_biblioitems_query,
                            groupby => 'homebranch',
                            additional_conditions => $self->negate([[
                                [@audience_enfants],
                                [@audience_adultes],
                            ]]),
                        },
                        {
                            title => 'Z202',
                            label => "Exemplaires ayant un type de document non-identifié",
                            based_query => $total_item_join_biblioitems_query,
                            groupby => 'homebranch',
                            additional_conditions => $self->negate([[
                                [@livres_imprimes],
                                [@publications_en_serie_imprimees],
                                [@microformes],
                                [@documents_cartographiques],
                                [@musique_imprimee],
                                [@documents_graphiques],
                                [@documents_sonores_musiques],
                                [@documents_sonores_livres_enregistres],
                                [@documents_video],
                                [@documents_multimedia],
                                [@livres_numeriques],
                                [@autres_documents],
                            ]]),
                        },
                    ],
                },
                {
                    title => 'Z3 - Adhérents',
                    based_query => $total_borrowers_by_categorycode_query,
                    groupby => 'categorycode',
                    queries => [
                        {
                            title => 'Z301',
                            label => "Adhérents sans date de naissance (non-collectivités)",
                            additional_conditions => [
                                [q|dateofbirth=NULL|, q|dateofbirth!=_%|],
                                q|category_type!=C|,
                            ],
                        },
                        {
                            title => 'Z302',
                            label => "Adhérents sans sexe (non-collectivités)",
                            additional_conditions => [
                                q|sex!=F|,
                                q|sex!=M|,
                                q|category_type!=C|,
                            ],
                        },
                        {
                            title => 'Z303',
                            label => "Adhérents sans sexe et sans date de naissance (non-collectivités)",
                            additional_conditions => [
                                [q|dateofbirth=NULL|, q|dateofbirth!=_%|],
                                q|sex!=F|,
                                q|sex!=M|,
                                q|category_type!=C|,
                            ],
                        },
                    ],
                },
            ],
        },
    ];
    return $blocks;
}

1;
