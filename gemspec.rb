GEM_NAME = 'brbackup'
GEM_FILES = FileList['**/*'] - FileList['coverage', 'coverage/**/*', 'pkg', 'pkg/**/*']
GEM_SPEC = Gem::Specification.new do |s|
  # == CONFIGURE ==
  s.author = "Tung Nguyen"
  s.email = "tongueroo@gmail.com"
  s.homepage = "http://github.com/tongueroo/#{GEM_NAME}"
  s.summary = "brbackup summary"
  # == CONFIGURE ==
  s.executables = ["brbackup"]
  s.add_dependency('builder', '>=2.1.2')
  s.extra_rdoc_files = [ "README.markdown" ]
  s.files = GEM_FILES.to_a
  s.has_rdoc = false
  s.name = GEM_NAME
  s.platform = Gem::Platform::RUBY
  s.require_path = "lib"
  s.version = "0.1.1"
end
