# generates fake analyses into xml files

import random
import subprocess
from firehose.model import Analysis, Metadata, Generator, Issue, \
    Location, Message, DebianSource, File, Function


analysis_tools = [("coccinelle", "1.0"),
                  ("coccinelle", "spatch version 1.0.0-rc15 with Python support and with PCRE support"),
                  ("clang", "1.2.3"),
                  ("cool-analysis-tool", "0.1")]


software = [("software 1", "1.0"),
            ("software 1", "2.0"),
            ("awesome app", "rc1"),
            ("another program", "8"),
            ("another program", "9"),
            ("hello", "1.0")]

functions = [("hello.c", "hello_world()"),
             ("run.py", "generate_test(var=8)"),
             ("foo.cpp", "string_from_int(integer)")]

if __name__ == "__main__":
    analyses = []
    for tool, tool_version in analysis_tools:
        for sut, sut_version in software:
            issues = []
            metadata = Metadata(Generator(tool, tool_version),
                                DebianSource(sut, sut_version, None),
                                None, None)
            for i in range(random.randint(0, 4)):
                file_, function = functions[random.randint(0, len(functions)-1)]
                file_ = File(file_, None, None)
                function = Function(function)
                location = Location(file_, function)
                fortune = subprocess.check_output(["fortune"])
                message = Message(fortune)
                issue = Issue(None, None, location, message, None, None)
                issues.append(issue)
            analysis = Analysis(metadata, issues)
            analyses.append(analysis)
    i = 0
    for analysis in analyses:
        f = open("analysis" + str(i) + ".xml", "w")
        f.write(analysis.to_xml_bytes())
        f.close()
        i += 1
