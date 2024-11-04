project('pollqueue', 'c')

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
