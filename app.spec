%define venv %{buildroot}/opt/%{name}
%define envdir /opt/%{name}
%define _p3 %{venv}/bin/python3

Name:           app
Version:        %{?pyversion}
Release:        1%{?dist}
Summary:        Training demo application

License:        Inmanta
URL:            https://code.inmanta.com/training/demo-app
Source0:        app-%{version}.tar.gz

%if 0%{?rhel}
BuildRequires:  python34-devel
BuildRequires:  python34-pip
Requires:       python34
%else
BuildRequires:  python3-devel
BuildRequires:  python3-pip
Requires:       python3
%endif

Requires:       graphviz

%description


%prep
%setup -q

%build

%install
rm -rf %{buildroot}
mkdir -p %{venv}

%{__python3} -m venv %{venv}

%{_p3} -m pip install -U wheel setuptools virtualenv pip
%{_p3} -m pip install -U %{SOURCE0}
rm %{venv}/pip-selfcheck.json

# Use the correct python for bytecompiling
%define __python %{_p3}

# Fix shebang
sed -i "s|%{buildroot}||g" %{venv}/bin/*
find %{venv} -name RECORD | xargs sed -i "s|%{buildroot}||g"

# Put symlinks
mkdir -p %{buildroot}%{_bindir}
ln -s %{envdir}/bin/%{name} %{buildroot}%{_bindir}/%{name}

#cp config.toml.example %{buildroot}/etc/repomanager.toml

%files
%{envdir}/bin
%{envdir}/lib
%{envdir}/lib64
%{envdir}/include
%{envdir}/pyvenv.cfg
%{_bindir}/%{name}
#%attr(-, repomanager, repomanager) %{_localstatedir}/lib/repomanager
#%config %attr(-, repomanager, repomanager) /etc/repomanager.toml

%pre
#getent group repomanager >/dev/null || groupadd -r repomanager
#getent passwd repomanager >/dev/null || useradd -r -g repomanager -d /var/lib/repomanager -s /bin/bash -c "Account used by the repomanager" repomanager
#exit

%changelog
* Fri Jun  9 2017 Bart Vanbrabant <bart.vanbrabant@inmanta.com>
- Initial package
