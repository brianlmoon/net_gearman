<?php

use \Phan\Config;

/**
 * This configuration will be read and overlayed on top of the
 * default configuration. Command line arguments will be applied
 * after this file is read.
 *
 * @see src/Phan/Config.php
 * See Config for all configurable options.
 *
 * A Note About Paths
 * ==================
 *
 * Files referenced from this file should be defined as
 *
 * ```
 *   Config::projectPath('relative_path/to/file')
 * ```
 *
 * where the relative path is relative to the root of the
 * project which is defined as either the working directory
 * of the phan executable or a path passed in via the CLI
 * '-d' flag.
 */
return [
    // If true, missing properties will be created when
    // they are first seen. If false, we'll report an
    // error message.
    'allow_missing_properties' => true,

    // Allow null to be cast as any type and for any
    // type to be cast to null.
    'null_casts_as_any_type' => true,

    // If this has entries, scalars (int, float, bool, string, null)
    // are allowed to perform the casts listed.
    // E.g. ['int' => ['float', 'string'], 'float' => ['int'], 'string' => ['int'], 'null' => ['string']]
    // allows casting null to a string, but not vice versa.
    // (subset of scalar_implicit_cast)
    'scalar_implicit_partial' => [
        'int'    => ['float', 'string'],
        'float'  => ['int'],
        'string' => ['int'],
        'null'   => ['string', 'bool'],
        'bool'   => ['null'],
    ],

    // Backwards Compatibility Checking
    'backward_compatibility_checks' => false,

    // Run a quick version of checks that takes less
    // time
    'quick_mode' => true,

    // Only emit critical issues
    'minimum_severity' => 0,

    // A set of fully qualified class-names for which
    // a call to parent::__construct() is required
    'parent_constructor_required' => [
    ],

    // Add any issue types (such as 'PhanUndeclaredMethod')
    // here to inhibit them from being reported
    'suppress_issue_types' => [
        // These report false positives in libraries due
        // to them not being used by any of the other
        // library code.
        'PhanUnreferencedPublicClassConstant',
        'PhanWriteOnlyProtectedProperty',
        'PhanUnreferencedPublicMethod',
        'PhanUnreferencedUseNormal',
        'PhanUnreferencedProtectedMethod',
        'PhanUnreferencedProtectedProperty',

    ],

    // A list of directories that should be parsed for class and
    // method information. After excluding the directories
    // defined in exclude_analysis_directory_list, the remaining
    // files will be statically analyzed for errors.
    //
    // Thus, both first-party and third-party code being used by
    // your application should be included in this list.
    'directory_list' => [
        'Net',
        'vendor',
        'tests',
    ],

    // A list of directories holding code that we want
    // to parse, but not analyze
    'exclude_analysis_directory_list' => [
        'vendor',
        'tests',
    ],

    // A file list that defines files that will be excluded
    // from parsing and analysis and will not be read at all.
    //
    // This is useful for excluding hopelessly unanalyzable
    // files that can't be removed for whatever reason.
    'exclude_file_list' => [
        'vendor/php-parallel-lint/php-parallel-lint/src/polyfill.php',
    ],

    // Set to true in order to attempt to detect dead
    // (unreferenced) code. Keep in mind that the
    // results will only be a guess given that classes,
    // properties, constants and methods can be referenced
    // as variables (like `$class->$property` or
    // `$class->$method()`) in ways that we're unable
    // to make sense of.
    'dead_code_detection' => true,
];
