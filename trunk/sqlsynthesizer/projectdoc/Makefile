PDF_FIGURES = workflow.pdf sql_skeleton.pdf

# Ensure that fonts are embedded in the resulting PDF file
EPSTOPDF = GS_OPTIONS="-dEmbedAllFonts=true -dPDFSETTINGS=/printer" epstopdf

pdf: compile ${PDF_FIGURES}
	pdflatex main.tex
	pdflatex main.tex

compile:
	latex main.tex
	bibtex main 
	latex main.tex
	latex main.tex

ps: compile
	dvips main

## Using pdflatex instead of dvipdf saves 9 lines.
# pdf: compile ${PDF_FIGURES}
# 	dvipdf main.dvi main.pdf

all: ps pdf

TAGS: tags
tags:
	etags `latex-process-inputs -list main.tex`

clean:
	rm -f main.aux
	rm -f main.bbl
	rm -f main.blg
	rm -f main.dvi
	rm -f main.log
	rm -f main.pdf
	rm -f main.ps
	rm -f TAGS

workflow.pdf: workflow.eps
	${EPSTOPDF} $<

sql_skeleton.pdf: sql_skeleton.eps
	${EPSTOPDF} $<
