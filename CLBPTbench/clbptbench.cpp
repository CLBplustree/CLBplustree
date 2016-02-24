#include <cstdio>
#include <cstdlib>
#include <cstring>
//#include <windows.h>
#include <fcntl.h>
#include <getopt.h>
#include <ctime>
#include <random>
#include <algorithm>

#define UNKNOWN_METHOD -1
#define UNIFORM_METHOD 0
#define NORMAL_METHOD 1
#define RANDOM_METHOD 2
#define FILLALL_METHOD 3
#define VERBOSE(x) if(x)printf




typedef struct
{
	int max;
	int min;
	int mean;
	int stddev;
	int verbose;
	int method;
	FILE* output;
	int insert_num;
	int select_num;
	int delete_num;
} optset;

typedef struct
{
	int *insertbuf;
	int *selectbuf;
	int *deletebuf;
	int insert_num;
	int select_num;
	int delete_num;
} outset;

void ArgParse(int ArgNum, char **ArgStr);
void helpmsg();
void optmsg(optset *opts);
void optcheck(optset *opts);
int str2method(char *str);
char *method2str(int method);
void gendata(optset *opts);
outset *uniform_method(optset *opts);
outset *normal_method(optset *opts);
outset *random_method(optset *opts);
outset *fillall_method(optset *opts);
void VisualResult(outset *);


using namespace std;

int main(int argc, char **argv)
{
	int i,ArgNum = 0;
	size_t size;
	char *ArgStr[128],*line=(char *)calloc(1024,sizeof(char));
	srand(time(NULL));
	ArgParse(argc,argv);
	return 0;
}

void ArgParse(int ArgNum, char **ArgStr)
{
	ArgStr[ArgNum+1]=NULL;
	optset opts;
    opts.max = 65535;
    opts.min = 0;
    opts.mean = 0;
    opts.method = UNIFORM_METHOD;
    opts.stddev = 0;
    opts.verbose = 0;
    opts.output = stdout;
    opts.insert_num = 0;
    opts.select_num = 0;
    opts.delete_num = 0;

	while(1)
	{
		char arg;
		int opt_index = 0;
		static struct option l_options[] =
		{
		{"help",no_argument,0,'h'},
		{"max",required_argument,0,'M'},
		{"min",required_argument,0,'m'},
		{"mean",required_argument,0,'A'},
		{"method",required_argument,0,'E'},
		{"stddev",required_argument,0,'S'},
		{"output",required_argument,0,'o'},
		{"verbose",no_argument,0,'v'},
		{"insert",required_argument,0,'i'},
		{"select",required_argument,0,'s'},
		{"delete",required_argument,0,'d'},
		{0,0,0,0}
		};
		if((arg = getopt_long(ArgNum,ArgStr,"A:S:E:hM:m:o:vi:s:d:",l_options,&opt_index))==-1)
		{
			if(opts.verbose)
				optmsg(&opts);
			optcheck(&opts);
			gendata(&opts);
			if(opts.output!=stdout)fclose(opts.output);
			break;
/*
			ArgNum = 0;
			printf("read argument\n");
			ArgStr[0]=(char *)"Argument";
			getline(&line,&size,stdin);
			ArgStr[1]=strtok(line," ");
			for(ArgNum = 2; ArgNum < 128 ; ArgNum++)
			{
				if((ArgStr[ArgNum]=strtok(NULL," "))==NULL)break;
			}
			ArgStr[ArgNum+1]=NULL;
			printf("end\n");
*/
		}
		switch(arg)
		{
			case 'A' :
				opts.mean = atoi(optarg);
			break;
			case 'S' :
				opts.stddev = atoi(optarg);
			break;
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
			case 'o' :
				opts.output = fopen(optarg,"w+b");
			break;
			case 'v' :
				opts.verbose = 1;
			break;
			case 'i' :
				opts.insert_num = atoi(optarg);
			break;
			case 's' :
				opts.select_num = atoi(optarg);
			break;
			case 'd' :
				opts.delete_num = atoi(optarg);
			break;
			default :
			break;
		}
	}
}

void helpmsg()
{
	printf("This is the CLBPT bench helper\n");
	printf("==============================\n");
	printf("--output  -o (str)              \n");
	printf("--max     -M (int)              \n");
	printf("--min     -m (int)              \n");
	printf("--mean    -A (int)              \n");
	printf("--stddev  -S (int)              \n");
	printf("--num     -n (int)              \n");
	printf("--method  -E (str)              \n");
	printf("             uniform            \n");
	printf("             normal             \n");
	printf("             random             \n");
	printf("             fillall            \n");
	printf("--verbose -v                    \n");
	printf("--insert  -i (int)              \n");
	printf("--select  -s (int)              \n");
	printf("--delete  -d (int)              \n");
	printf("--help    -h                    \n");
}

void optmsg(optset *opts)
{
	printf("The bench setting list :\n");
	printf("========================\n");
	printf("Max index : %10d\n",opts->max);
	printf("Min index : %10d\n",opts->min);
	printf("Mean      : %10d\n",opts->mean);
	printf("Data num  : %10d\n",opts->insert_num+opts->select_num+opts->delete_num);
	printf("Method    : %10s\n",method2str(opts->method));
	printf("\n");
}

void optcheck(optset *opts)
{
	if( opts->delete_num > opts->insert_num )
	{
		fprintf(stderr,"Insert operation must not be less than delete operation.\n");
		fprintf(stderr,"STOP GENERATION!\n");
		exit(1);
	}
}

int str2method(char *str)
{
	if(!strcmp(str,"normal"))
		return NORMAL_METHOD;
	if(!strcmp(str,"uniform"))
		return UNIFORM_METHOD;
	if(!strcmp(str,"fillall"))
		return FILLALL_METHOD;
	return UNKNOWN_METHOD;
}

char *method2str(int method)
{
	switch(method)
	{
		case UNIFORM_METHOD :
		return (char *)"UNIFORM_METHOD";
		case NORMAL_METHOD :
		return (char *)"NORMAL_METHOD";
		case FILLALL_METHOD :
		return (char *)"FILLALL_METHOD";
		default :
		return (char *)"UNKNOWN_METHOD";
	}
}

void gendata(optset *opts)
{
	outset *outbuf;
	switch( opts->method )
	{
		case UNIFORM_METHOD :
			outbuf = uniform_method(opts);
		break;
		case NORMAL_METHOD :
			outbuf = normal_method(opts);
		break;
		case RANDOM_METHOD :
			outbuf = random_method(opts);
		break;
		case FILLALL_METHOD :
			outbuf = fillall_method(opts);
		break;
		default :
			perror("Unknown Method");
		exit(-1);
	}
	if(opts->verbose)
		VisualResult(outbuf);
}

void VisualResult(outset *outbuf)
{
	int graph[10] = {0};
	int *buf = outbuf->insertbuf;
	int num = outbuf->insert_num;
	if( num < 10 )
	{
		fprintf(stderr,"Unable Visualize\n");
		return;
	}
	sort(buf,buf+num);
	int interval = buf[num-1] - buf[0];
	for( int i = 0, j = 0 ,x = buf[0]+interval/10 ; x <= buf[num-1] ; x+=interval/10, j++ )
	{
		graph[j] = 0;
		for( ; buf[i] < x ; i++ )
		{
			graph[j]++;
		}
	}
	for( int i = 0 ; i < 10 ; i++ )
		printf("[%8d] ~ [%8d] %10d\n",buf[0]+i*interval/10,buf[0]+(i+1)*interval/10,graph[i]);
}

outset *uniform_method(optset *opts)
{
    int type = 0;
	int isz = opts->insert_num, ssz = opts->select_num, dsz = opts->delete_num;
	outset *outbuf = (outset *)malloc(sizeof(outset));
	outbuf->insertbuf = (int *)calloc(sizeof(int),opts->insert_num);
	outbuf->selectbuf = (int *)calloc(sizeof(int),opts->select_num);
	outbuf->deletebuf = (int *)calloc(sizeof(int),opts->delete_num);
	outbuf->insert_num = opts->insert_num;
	outbuf->select_num = opts->select_num;
	outbuf->delete_num = opts->delete_num;
	random_device rd;
	mt19937_64 gen(rd());
	uniform_int_distribution<> dis(opts->min,opts->max);
	VERBOSE(opts->verbose)("Generating %10d insert operations\n",isz);

    if(opts->insert_num){
            type = 1;
            fprintf(opts->output, "%d %d\n", type, opts->insert_num);
            /*
            fwrite(&type,sizeof(int), 1,opts->output);
            fwrite(&opts->insert_num,sizeof(int), 1,opts->output);
            */
    }
	for( int i = 0 ; i < opts->insert_num ; i++ )
	{
		outbuf->insertbuf[i] = dis(gen);
		fprintf(opts->output, "%d %d\n", outbuf->insertbuf[i], 
			outbuf->insertbuf[i]);
		//fwrite(&outbuf->insertbuf[i],sizeof(int),1,opts->output);
		//fwrite(&outbuf->insertbuf[i],sizeof(int),1,opts->output);
	}
	VERBOSE(opts->verbose)("Generating %10d select operations\n",ssz);

    if(opts->select_num){
            type = 2;
	    fprintf(opts->output, "%d %d\n", type, opts->select_num);
	    /*
            fwrite(&type,sizeof(int), 1,opts->output);
            fwrite(&opts->select_num,sizeof(int), 1,opts->output);
	    */
    }
	for( int i = 0 ; i < opts->select_num ; i++ )
	{
		outbuf->selectbuf[i] = dis(gen);
		fprintf(opts->output, "%d\n", outbuf->selectbuf[i]);
		//fwrite(&outbuf->selectbuf[i],sizeof(int),1,opts->output);
		//printf("%d key : %d.\n",i,outbuf->selectbuf[i]);
	}
	VERBOSE(opts->verbose)("Generating %10d delete operations\n",dsz);
	if(opts->delete_num)fprintf(opts->output,"%d %d\n",3,opts->delete_num);
	for( int i = 0 ; i < opts->delete_num ; i++ )
	{
		outbuf->deletebuf[i] = dis(gen);
		fprintf(opts->output,"%d\n",outbuf->deletebuf[i]);
	}
	for( int i = 0 ; i < opts->delete_num ; i++ )
	{
		fprintf(opts->output,"%d\n",outbuf->deletebuf[i]);
	}
	return outbuf;
}

outset *normal_method(optset *opts)
{
    int type = 0;
	int isz = opts->insert_num, ssz = opts->select_num, dsz = opts->delete_num;
	outset *outbuf = (outset *)malloc(sizeof(outset));
	outbuf->insertbuf = (int *)calloc(sizeof(int),opts->insert_num);
	outbuf->selectbuf = (int *)calloc(sizeof(int),opts->select_num);
	outbuf->deletebuf = (int *)calloc(sizeof(int),opts->delete_num);
	outbuf->insert_num = opts->insert_num;
	outbuf->select_num = opts->select_num;
	outbuf->delete_num = opts->delete_num;
	random_device rd;
	mt19937_64 gen(rd());
	normal_distribution<> dis(opts->mean, opts->stddev);
	VERBOSE(opts->verbose)("Generating %10d insert operations\n",isz);

    if(opts->insert_num){
            type = 1;
            fprintf(opts->output, "%d %d\n", type, opts->insert_num);
            /*
            fwrite(&type,sizeof(int), 1,opts->output);
            fwrite(&opts->insert_num,sizeof(int), 1,opts->output);
            */
    }
	for( int i = 0 ; i < opts->insert_num ; i++ )
	{
		outbuf->insertbuf[i] = dis(gen);
		fprintf(opts->output, "%d %d\n", outbuf->insertbuf[i], 
			outbuf->insertbuf[i]);
		//fwrite(&outbuf->insertbuf[i],sizeof(int),1,opts->output);
		//fwrite(&outbuf->insertbuf[i],sizeof(int),1,opts->output);
	}
	VERBOSE(opts->verbose)("Generating %10d select operations\n",ssz);

    if(opts->select_num){
            type = 2;
	    fprintf(opts->output, "%d %d\n", type, opts->select_num);
	    /*
            fwrite(&type,sizeof(int), 1,opts->output);
            fwrite(&opts->select_num,sizeof(int), 1,opts->output);
	    */
    }
	for( int i = 0 ; i < opts->select_num ; i++ )
	{
		outbuf->selectbuf[i] = dis(gen);
		fprintf(opts->output, "%d\n", outbuf->selectbuf[i]);
		//fwrite(&outbuf->selectbuf[i],sizeof(int),1,opts->output);
		//printf("%d key : %d.\n",i,outbuf->selectbuf[i]);
	}
	VERBOSE(opts->verbose)("Generating %10d delete operations\n",dsz);
	if(opts->delete_num)fprintf(opts->output,"%d %d\n",3,opts->delete_num);
	for( int i = 0 ; i < opts->delete_num ; i++ )
	{
		outbuf->deletebuf[i] = dis(gen);
		fprintf(opts->output,"%d\n",outbuf->deletebuf[i]);
	}
	for( int i = 0 ; i < opts->delete_num ; i++ )
	{
		fprintf(opts->output,"%d\n",outbuf->deletebuf[i]);
	}
	return outbuf;
}

outset *random_method(optset *opts)
{
	int i, isz = opts->insert_num, ssz = opts->select_num, dsz = opts->delete_num;
	outset *outbuf = (outset *)malloc(sizeof(outset));
	int *box = (int *)calloc(isz,sizeof(int));
	VERBOSE(opts->verbose)("Generating %10d insert operations\n",isz);
	for(i=0;i<isz;i++)
	{
		outbuf->insertbuf[i] = (rand()%(opts->max-opts->min+1)+opts->min);
		fprintf(opts->output,"i %d\n",outbuf->insertbuf[i] );
		box[i] = outbuf->insertbuf[i] ;
	}
	VERBOSE(opts->verbose)("Generating %10d select operations\n",ssz);
	for(i=0;i<ssz;i++)
	{
		outbuf->selectbuf[i]  = (rand()%isz);
		fprintf(opts->output,"s %d\n",box[outbuf->selectbuf[i]]);
	}
	VERBOSE(opts->verbose)("Generating %10d delete operations\n",dsz);
	for(i=0;i<dsz;i++)
	{
		outbuf->deletebuf[i] = (rand()%isz);
		fprintf(opts->output,"d %d\n",box[outbuf->deletebuf[i]]);
		box[outbuf->deletebuf[i]] = box[--isz];
	}
	return outbuf;
}

outset *fillall_method(optset *opts)
{
    int type = 0;
	outset *outbuf = (outset *)malloc(sizeof(outset));
	outbuf->insertbuf = (int *)calloc(sizeof(int),opts->max - opts->min + 1);
	outbuf->selectbuf = NULL;
	outbuf->deletebuf = NULL;
	outbuf->insert_num = opts->max - opts->min + 1;
	outbuf->select_num = 0;
	outbuf->delete_num = 0;
	VERBOSE(opts->verbose)("Generating %10d insert operations\n",
		opts->max - opts->min + 1);

    {
            type = 1;
            fprintf(opts->output, "%d %d\n", type, opts->max - opts->min + 1);
            /*
            fwrite(&type,sizeof(int), 1,opts->output);
            fwrite(&opts->insert_num,sizeof(int), 1,opts->output);
            */
    }
	for( int i = 0; i < opts->max - opts->min + 1; i++ )
	{
		outbuf->insertbuf[i] = i + opts->min;
	}
	random_shuffle(outbuf->insertbuf, 
		outbuf->insertbuf + (opts->max - opts->min + 1));
	for( int i = 0; i < opts->max - opts->min + 1; i++ )
	{
		fprintf(opts->output, "%d %d\n", outbuf->insertbuf[i], 
			outbuf->insertbuf[i]);
	}
	return outbuf;
}
