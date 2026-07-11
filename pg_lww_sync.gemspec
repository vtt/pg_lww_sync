require_relative "lib/pg_lww_sync/version" if File.exist?(File.expand_path("lib/pg_lww_sync/version.rb", __dir__))

Gem::Specification.new do |spec|
  spec.name          = "pg_lww_sync"
  spec.version       = defined?(PgLwwSync::VERSION) ? PgLwwSync::VERSION : "0.1.0"
  spec.authors       = ["Tin Vo"]
  spec.email         = ["vtt999@gmail.com"]

  spec.summary       = "Self-contained application-level active-active replication engine for PostgreSQL."
  spec.description   = "An asynchronous application outbox engine using a microsecond-accurate Last-Write-Wins (LWW) column matrix to safely synchronize distributed PostgreSQL database nodes without native replication infrastructure."
  spec.homepage      = "https://github.com/dhammaorg/pg_lww_sync"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  # Specify which files should be added to the gem when it is packaged
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:test|spec|features)/})
    end
  end
  
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Core runtime dependencies
  spec.add_dependency "activerecord", ">= 6.1"
  spec.add_dependency "railties",     ">= 6.1"
  spec.add_dependency "pg",           ">= 1.3"

  # Development dependencies
  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake",    "~> 13.0"
end
