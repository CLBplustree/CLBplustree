#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <time.h>

#define UNKNOWN_METHOD -1
#define NORMAL_METHOD 0

typedef struct
{
	int max;
	int min;
	int num;
	int verbose;
	int method;
	int output;
	float insert_ratio;
	float select_ratio;
	float delete_ratio;
} optset;

void helpmsg();
void optmsg(optset *opts);
void optcheck(optset *opts);
int str2method(char *str);
char *method2str(int method);
void gendata(optset *opts);

int main(int argc, char **argv)
{
	static optset opts = 
	{
		.max = 65535,
		.min = 0,
		.num = 0,
		.method = NORMAL_METHOD,
		.verbose = 0,
		.output = STDOUT_FILENO,
		.insert_ratio = 40,
		.select_ratio = 30,
		.delete_ratio = 30
	};
	if( argc <= 1)
	{
		helpmsg();
		exit(0);
	}
	srand(time(NULL));
	while(1)
	{
		char arg;
		int opt_index = 0;
		static struct option l_options[] =
		{
		{"help",no_argument,0,'h'},
		{"max",required_argument,0,'M'},
		{"min",required_argument,0,'m'},
		{"num",required_argument,0,'n'},
		{"method",required_argument,0,'E'},
		{"output",required_argument,0,'o'},
		{"verbose",no_argument,0,'v'},
		{0,0,0,0}
		};
		if((arg = getopt_long(argc,argv,"E:hM:m:n:o:v",l_options,&opt_index))==-1)
			break;
		switch(arg)
		{
			case 'h' :
				helpmsg();
				exit(0);
			break;
			case 'M' :
				opts.max = atoi(optarg); 
			break;
			case 'm' :
				opts.min = atoi(optarg); 
			break;
			case 'E' :
				opts.method = str2method(optarg);
			break;
			case 'n' :
				opts.num = atoi(optarg);
			break;
			case 'o' :
				opts.output = open(optarg,O_RDWR|O_CREAT,0644);
				if(opts.output == -1)perror("open file error");
			break;
			case 'v' :
				opts.verbose = 1; 
			break;
			default :
			break;
		}
	}

	if(opts.verbose)
		optmsg(&opts);
	optcheck(&opts);
	gendata(&opts);
	close(opts.output);

	return 0;
}

void helpmsg()
{
	printf("This is the CLBPT bench helper\n");
	printf("==============================\n");
	printf("--output  -o (str)                \n");
	printf("--max     -M (int)                \n");
	printf("--min     -m (int)                \n");
	printf("--num     -n (int)                \n");
	printf("--method  -E (str)                \n");
	printf("             normal               \n");
	printf("--verbose -v                    \n");
	printf("--help    -h                    \n");
}

void optmsg(optset *opts)
{
	printf("The bench setting list :\n");
	printf("========================\n");
	printf("Max index : %10d\n",opts->max);
	printf("Min index : %10d\n",opts->min);
	printf("Data size : %10d\n",opts->num);
	printf("Method    : %10s\n",method2str(opts->method));
	printf("\n");
}

void optcheck(optset *opts)
{
	if( opts->delete_ratio > opts->insert_ratio )
	{
		fprintf(stderr,"Insert operation must not be less than delete operation.\n");
		fprintf(stderr,"STOP GENERATION!\n");
		exit(1);
	}
}

int str2method(char *str)
{
	if(!strcasecmp(str,"normal"))
		return NORMAL_METHOD;
	return UNKNOWN_METHOD;
}

char *method2str(int method)
{
	switch(method)
	{
		case NORMAL_METHOD :
		return "NORMAL_METHOD";
		default :
		return "UNKNOWN_METHOD";
	}
}

void gendata(optset *opts)
{
	int i, isz, ssz, dsz;
	int *box;
	isz = ((opts->num * opts->insert_ratio) / 100);
	box = calloc(isz,sizeof(int));
	if(opts->verbose)printf("Generating %10d insert operations\n",isz);
	for(i=0;i<isz;i++)
	{
		int n = (rand()%(opts->max-opts->min)+opts->min);
		dprintf(opts->output,"i %d\n",n);
		box[i] = n;
	}
	ssz = ((opts->num * opts->select_ratio) / 100);
	if(opts->verbose)printf("Generating %10d select operations\n",ssz);
	for(i=0;i<ssz;i++)
	{
		int n = (rand()%isz);
		dprintf(opts->output,"s %d\n",box[n]);
	}
	dsz = ((opts->num * opts->delete_ratio) / 100);
	if(opts->verbose)printf("Generating %10d delete operations\n",dsz);
	for(i=0;i<dsz;i++)
	{
		int n = (rand()%isz);
		dprintf(opts->output,"d %d\n",box[n]);
	}
}

