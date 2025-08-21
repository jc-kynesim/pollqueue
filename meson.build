project('pollqueue', 'c',
	default_options: ['warning_level=3',
		'werror=true',
		'buildtype=debugoptimized',
		'default_library=static']
)

pollqueue_deps = [ dependency('threads') ]

pollqueue_lib = library('pollqueue',
	'pollqueue.c',
	dependencies : pollqueue_deps,
	install : true)

pollqueue_dep = declare_dependency(
	link_with : pollqueue_lib,
	dependencies : pollqueue_deps,
	include_directories : [ include_directories('.') ])

meson.override_dependency('pollqueue', pollqueue_dep)

if not meson.is_subproject()
    subdir('test')
endif
