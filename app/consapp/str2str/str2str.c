/*------------------------------------------------------------------------------
* str2str.c : console version of stream server
*
*          Copyright (C) 2007-2020 by T.TAKASU, All rights reserved.
*
* version : $Revision: 1.1 $ $Date: 2008/07/17 21:54:53 $
* history : 2009/06/17  1.0  new
*           2011/05/29  1.1  add -f, -l and -x option
*           2011/11/29  1.2  fix bug on recognize ntrips:// (rtklib_2.4.1_p4)
*           2012/12/25  1.3  add format conversion functions
*                            add -msg, -opt and -sta options
*                            modify -p option
*           2013/01/25  1.4  fix bug on showing message
*           2014/02/21  1.5  ignore SIG_HUP
*           2014/08/10  1.5  fix bug on showing message
*           2014/08/26  1.6  support input format gw10, binex and rt17
*           2014/10/14  1.7  use stdin or stdout if option -in or -out omitted
*           2014/11/08  1.8  add option -a, -i and -o
*           2015/03/23  1.9  fix bug on parsing of command line options
*           2016/01/23  1.10 enable septentrio
*           2016/01/26  1.11 fix bug on station position by -p option (#126)
*                            add option -px
*           2016/07/01  1.12 support CMR/CMR+
*           2016/07/23  1.13 add option -c1 -c2 -c3 -c4
*           2016/09/03  1.14 support ntrip caster
*                            add option -ft,-fl
*           2016/09/06  1.15 add reload soure table by USR2 signal
*           2016/09/17  1.16 add option -b
*           2017/05/26  1.17 add input format tersus
*           2020/11/30  1.18 support api change strsvrstart(),strsvrstat()
*           2024/xx/xx  1.19 add -pg (bidirectional serial + NMEA pipe)
*                            add -nmea host:port (TCP NMEA server)
*-----------------------------------------------------------------------------*/
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <math.h>
#include "rtklib.h"

#define PRGNAME     "str2str"          /* program name */
#define MAXSTR      5                  /* max number of streams */
#define TRFILE      "str2str.trace"    /* trace file */
#define NMEA_MAX_CLIENTS 8             /* max TCP NMEA clients */

/* global variables ----------------------------------------------------------*/
static strsvr_t strsvr;                /* stream server */
static volatile int intrflg=0;         /* interrupt flag */

/* NMEA TCP server state */
static volatile int gps_ready=0;
static int nmeatcp=-1;                 /* NMEA TCP server socket */
static int nmea_clients[NMEA_MAX_CLIENTS];
static int nmea_nclient=0;
static pthread_mutex_t nmea_mutex=PTHREAD_MUTEX_INITIALIZER;
static int nmea_pipe[2]={-1,-1};
static pthread_t nmea_tid=0;
static char nmea_svr_host[64]="0.0.0.0";
static int nmea_svr_port=0;

/* help text -----------------------------------------------------------------*/
static const char *help[]={
"",
" usage: str2str [-in stream] [-out stream [-out stream...]] [options]",
"",
" Input data from a stream and divide and output them to multiple streams",
" The input stream can be serial, tcp client, tcp server, ntrip client, or",
" file. The output stream can be serial, tcp client, tcp server, ntrip server,",
" or file. str2str is a resident type application. To stop it, type ctr-c in",
" console if run foreground or send signal SIGINT for background process.",
" if run foreground or send signal SIGINT for background process.",
" if both of the input stream and the output stream follow #format, the",
" format of input messages are converted to output. To specify the output",
" messages, use -msg option. If the option -in or -out omitted, stdin for",
" input or stdout for output is used. If the stream in the option -in or -out",
" is null, stdin or stdout is used as well.",
" Command options are as follows.",
"",
" -in  stream[#format] input  stream path and format",
" -out stream[#format] output stream path and format",
"",
"  stream path",
"    serial       : serial://port[:brate[:bsize[:parity[:stopb[:fctr]]]]]",
"    tcp server   : tcpsvr://:port",
"    tcp client   : tcpcli://addr[:port]",
"    ntrip client : ntrip://[user[:passwd]@]addr[:port][/mntpnt]",
"    ntrip server : ntrips://[:passwd@]addr[:port]/mntpnt[:str] (only out)",
"    ntrip caster : ntripc://[user:passwd@][:port]/mntpnt[:srctbl] (only out)",
"    file         : [file://]path[::T][::+start][::xseppd][::S=swap]",
"",
"  format",
"    rtcm2        : RTCM 2 (only in)",
"    rtcm3        : RTCM 3",
"    nov          : NovAtel OEMV/4/6,OEMStar (only in)",
"    oem3         : NovAtel OEM3 (only in)",
"    ubx          : ublox LEA-4T/5T/6T (only in)",
"    ss2          : NovAtel Superstar II (only in)",
"    hemis        : Hemisphere Eclipse/Crescent (only in)",
"    stq          : SkyTraq S1315F (only in)",
"    javad        : Javad (only in)",
"    nvs          : NVS BINR (only in)",
"    binex        : BINEX (only in)",
"    rt17         : Trimble RT17 (only in)",
"    sbf          : Septentrio SBF (only in)",
"",
" -msg \"type[(tint)][,type[(tint)]...]\"",
"                   rtcm message types and output intervals (s)",
" -sta sta          station id",
" -opt opt          receiver dependent options",
" -s  msec          timeout time (ms) [10000]",
" -r  msec          reconnect interval (ms) [10000]",
" -n  msec          nmea request cycle (m) [0]",
" -f  sec           file swap margin (s) [30]",
" -c  file          input commands file [no]",
" -c1 file          output 1 commands file [no]",
" -c2 file          output 2 commands file [no]",
" -c3 file          output 3 commands file [no]",
" -c4 file          output 4 commands file [no]",
" -p  lat lon hgt   station position (latitude/longitude/height) (deg,m)",
" -px x y z         station position (x/y/z-ecef) (m)",
" -a  antinfo       antenna info (separated by ,)",
" -i  rcvinfo       receiver info (separated by ,)",
" -o  e n u         antenna offset (e,n,u) (m)",
" -l  local_dir     ftp/http local directory []",
" -x  proxy_addr    http/ntrip proxy address [no]",
" -b  str_no        relay back messages from output str to input str [no]",
" -t  level         trace level [0]",
" -fl file          log file [str2str.trace]",
" -pg               enable bidirectional serial: read NMEA from -out serial stream",
" -nmea [host:]port  broadcast NMEA (from -pg) over TCP (e.g. 0.0.0.0:9999)",
" -h                print help",
};
/* print help ----------------------------------------------------------------*/
static void printhelp(void)
{
    int i;
    for (i=0;i<(int)(sizeof(help)/sizeof(*help));i++) fprintf(stderr,"%s\n",help[i]);
    exit(0);
}
/* signal handler ------------------------------------------------------------*/
static void sigfunc(int sig)
{
    intrflg=1;
}
/* decode format -------------------------------------------------------------*/
static void decodefmt(char *path, int *fmt)
{
    char *p;

    *fmt=-1;

    if ((p=strrchr(path,'#'))) {
        if      (!strcmp(p,"#rtcm2")) *fmt=STRFMT_RTCM2;
        else if (!strcmp(p,"#rtcm3")) *fmt=STRFMT_RTCM3;
        else if (!strcmp(p,"#nov"  )) *fmt=STRFMT_OEM4;
        else if (!strcmp(p,"#oem3" )) *fmt=STRFMT_OEM3;
        else if (!strcmp(p,"#ubx"  )) *fmt=STRFMT_UBX;
        else if (!strcmp(p,"#ss2"  )) *fmt=STRFMT_SS2;
        else if (!strcmp(p,"#hemis")) *fmt=STRFMT_CRES;
        else if (!strcmp(p,"#stq"  )) *fmt=STRFMT_STQ;
        else if (!strcmp(p,"#javad")) *fmt=STRFMT_JAVAD;
        else if (!strcmp(p,"#nvs"  )) *fmt=STRFMT_NVS;
        else if (!strcmp(p,"#binex")) *fmt=STRFMT_BINEX;
        else if (!strcmp(p,"#rt17" )) *fmt=STRFMT_RT17;
        else if (!strcmp(p,"#sbf"  )) *fmt=STRFMT_SEPT;
        else return;
        *p='\0';
    }
}
/* decode stream path --------------------------------------------------------*/
static int decodepath(const char *path, int *type, char *strpath, int *fmt)
{
    char buff[1024],*p;

    strcpy(buff,path);

    /* decode format */
    decodefmt(buff,fmt);

    /* decode type */
    if (!(p=strstr(buff,"://"))) {
        strcpy(strpath,buff);
        *type=STR_FILE;
        return 1;
    }
    if      (!strncmp(path,"serial",6)) *type=STR_SERIAL;
    else if (!strncmp(path,"tcpsvr",6)) *type=STR_TCPSVR;
    else if (!strncmp(path,"tcpcli",6)) *type=STR_TCPCLI;
    else if (!strncmp(path,"ntripc",6)) *type=STR_NTRIPCAS;
    else if (!strncmp(path,"ntrips",6)) *type=STR_NTRIPSVR;
    else if (!strncmp(path,"ntrip", 5)) *type=STR_NTRIPCLI;
    else if (!strncmp(path,"file",  4)) *type=STR_FILE;
    else {
        fprintf(stderr,"stream path error: %s\n",buff);
        return 0;
    }
    strcpy(strpath,p+3);
    return 1;
}
/* read receiver commands ----------------------------------------------------*/
static void readcmd(const char *file, char *cmd, int type)
{
    FILE *fp;
    char buff[MAXSTR],*p=cmd;
    int i=0;

    *p='\0';

    if (!(fp=fopen(file,"r"))) return;

    while (fgets(buff,sizeof(buff),fp)) {
        if (*buff=='@') i++;
        else if (i==type&&p+strlen(buff)+1<cmd+MAXRCVCMD) {
            p+=sprintf(p,"%s",buff);
        }
    }
    fclose(fp);
}
/* validate NMEA checksum ----------------------------------------------------*/
static int nmea_checksum_ok(const char *line)
{
    const char *p;
    char *star;
    unsigned char sum=0;
    unsigned int given;

    if (*line!='$') return 0;
    star=strchr(line,'*');
    if (!star||star-line<2) return 0;
    for (p=line+1;p<star;p++) sum^=(unsigned char)*p;
    return sscanf(star+1,"%2X",&given)==1&&sum==(unsigned char)given;
}
/* parse GGA sentence, return geodetic pos in pos[3] (rad,rad,m ellipsoidal) */
static int parse_gga(const char *line, double pos[3])
{
    double latdm=0.0,londm=0.0,alt=0.0,geoid=0.0;
    char ns='N',ew='E';
    int q=0;

    if (sscanf(line,"$%*5s,%*[^,],%lf,%c,%lf,%c,%d,%*d,%*f,%lf,%*c,%lf",
               &latdm,&ns,&londm,&ew,&q,&alt,&geoid)<6) return 0;
    if (q==0) return 0;

    pos[0]=((int)(latdm/100.0)+fmod(latdm,100.0)/60.0)*D2R;
    pos[1]=((int)(londm/100.0)+fmod(londm,100.0)/60.0)*D2R;
    pos[2]=alt+geoid;
    if (ns=='S') pos[0]=-pos[0];
    if (ew=='W') pos[1]=-pos[1];
    return 1;
}
/* broadcast NMEA line to all TCP clients ------------------------------------*/
static void nmea_broadcast(const char *line, int len)
{
    int i,j;

    pthread_mutex_lock(&nmea_mutex);
    for (i=j=0;i<nmea_nclient;i++) {
        if (send(nmea_clients[i],line,len,MSG_NOSIGNAL)>0) {
            nmea_clients[j++]=nmea_clients[i];
        } else {
            close(nmea_clients[i]);
        }
    }
    nmea_nclient=j;
    pthread_mutex_unlock(&nmea_mutex);
}
/* GPS position thread: reads NMEA from pipe, updates nmeapos, broadcasts ----*/
static void *gps_pos_thread(void *arg)
{
    int pipefd=*(int *)arg;
    char buf[4096],line[512];
    int buflen=0;
    ssize_t n;
    double geopos[3],ecef[3];

    fprintf(stderr,"[GPS] NMEA reader started (pipe from serial)\n");

    while (1) {
        char *p,*q;
        n=read(pipefd,buf+buflen,(size_t)(sizeof(buf)-buflen-1));
        if (n<=0) break;
        buflen+=(int)n;
        buf[buflen]='\0';
        p=buf;

        while ((q=memchr(p,'\n',buf+buflen-p))) {
            int len=(int)(q-p);
            if (len>0&&p[len-1]=='\r') len--;
            if (len>0&&len<(int)sizeof(line)-1) {
                memcpy(line,p,len);
                line[len]='\0';

                if (nmea_checksum_ok(line)) {
                    char out[520];
                    int olen=sprintf(out,"%s\r\n",line);
                    if (nmea_nclient>0) nmea_broadcast(out,olen);

                    if ((strncmp(line,"$GNGGA",6)==0||
                         strncmp(line,"$GPGGA",6)==0)&&
                        parse_gga(line,geopos)) {
                        pos2ecef(geopos,ecef);
                        lock(&strsvr.nmeapos_lock);
                        matcpy(strsvr.nmeapos,ecef,3,1);
                        strsvr.nmeapos_valid=1;
                        unlock(&strsvr.nmeapos_lock);
                        fprintf(stderr,"[GPS] pos updated: lat=%.6f lon=%.6f alt=%.1f\n",
                                geopos[0]/D2R,geopos[1]/D2R,geopos[2]);
                        if (!gps_ready) {
                            fprintf(stderr,"[GPS] initial position: lat=%.6f lon=%.6f alt=%.1f\n",
                                    geopos[0]/D2R,geopos[1]/D2R,geopos[2]);
                            gps_ready=1;
                        }
                    }
                }
            }
            p=q+1;
        }
        buflen=(int)(buf+buflen-p);
        if (buflen>0) memmove(buf,p,buflen);
    }
    return NULL;
}
/* NMEA TCP accept thread ----------------------------------------------------*/
static void *nmea_accept_thread(void *arg)
{
    int nmea_sid=*(int *)arg;
    struct sockaddr_in caddr;
    socklen_t caddrlen=sizeof(caddr);
    int cfd,opt=1;

    fprintf(stderr,"[NMEA] TCP server listening on %s:%d\n",
            nmea_svr_host,nmea_svr_port);

    while (!intrflg) {
        cfd=accept(nmea_sid,(struct sockaddr *)&caddr,&caddrlen);
        if (cfd<0) break;

        setsockopt(cfd,IPPROTO_TCP,TCP_NODELAY,&opt,sizeof(opt));

        pthread_mutex_lock(&nmea_mutex);
        if (nmea_nclient<NMEA_MAX_CLIENTS) {
            nmea_clients[nmea_nclient++]=cfd;
            fprintf(stderr,"@[NMEA] client +%s (%d total)\n",
                    inet_ntoa(caddr.sin_addr),nmea_nclient);
        } else {
            fprintf(stderr,"[NMEA] max clients reached, connection rejected\n");
            close(cfd);
        }
        pthread_mutex_unlock(&nmea_mutex);
    }
    return NULL;
}
/* start NMEA TCP server -----------------------------------------------------*/
static int nmea_server_start(const char *host, int port,
                              struct sockaddr_in *addr_out)
{
    struct sockaddr_in addr;
    int nmea_sid,opt=1;

    if ((nmea_sid=socket(AF_INET,SOCK_STREAM,0))<0) {
        fprintf(stderr,"nmea socket\n");
        return -1;
    }
    setsockopt(nmea_sid,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    memset(&addr,0,sizeof(addr));
    addr.sin_family=AF_INET;
    addr.sin_port=htons((uint16_t)port);
    addr.sin_addr.s_addr=inet_addr(host);

    if (bind(nmea_sid,(struct sockaddr *)&addr,sizeof(addr))<0) {
        fprintf(stderr,"nmea bind\n");
        close(nmea_sid);
        return -1;
    }
    if (listen(nmea_sid,8)<0) {
        fprintf(stderr,"nmea listen\n");
        close(nmea_sid);
        return -1;
    }
    if (addr_out) *addr_out=addr;
    return nmea_sid;
}
/* wait for initial GPS position from serial ---------------------------------*/
static void wait_initial_position(const char *serialdev)
{
    fprintf(stderr,"[GPS] waiting for initial position from %s ...\n",serialdev);
    while (!gps_ready&&!intrflg) sleepms(200);
}
/* str2str -------------------------------------------------------------------*/
int main(int argc, char **argv)
{
    static char cmd_strs[MAXSTR][MAXRCVCMD]={"","","","",""};
    static char cmd_periodic_strs[MAXSTR][MAXRCVCMD]={"","","","",""};
    const char ss[]={'E','-','W','C','C'};
    strconv_t *conv[MAXSTR]={NULL};
    double pos[3],stapos[3]={0},stadel[3]={0};
    static char s1[MAXSTR][MAXSTRPATH]={{0}},s2[MAXSTR][MAXSTRPATH]={{0}};
    char *paths[MAXSTR],*logs[MAXSTR];
    char *cmdfile[MAXSTR]={"","","","",""},*cmds[MAXSTR],*cmds_periodic[MAXSTR];
    char *local="",*proxy="",*msg="1004,1019",*opt="",buff[256],*p;
    char strmsg[MAXSTRMSG]="",*antinfo="",*rcvinfo="";
    char *ant[]={"","",""},*rcv[]={"","",""},*logfile="";
    int i,j,n=0,dispint=5000,trlevel=0,opts[]={10000,10000,2000,32768,10,0,30,0};
    int types[MAXSTR]={STR_FILE,STR_FILE},stat[MAXSTR]={0},log_stat[MAXSTR]={0};
    int byte[MAXSTR]={0},bps[MAXSTR]={0},fmts[MAXSTR]={0},sta=0;
    int pgflag=0;
    int stapos_set=0;
    char serialdev[256]="";
    pthread_t gps_tid=0;

    for (i=0;i<MAXSTR;i++) {
        paths[i]=s1[i];
        logs[i]=s2[i];
        cmds[i]=cmd_strs[i];
        cmds_periodic[i]=cmd_periodic_strs[i];
    }
    for (i=1;i<argc;i++) {
        if (!strcmp(argv[i],"-in")&&i+1<argc) {
            if (!decodepath(argv[++i],types,paths[0],fmts)) return -1;
        }
        else if (!strcmp(argv[i],"-out")&&i+1<argc&&n<MAXSTR-1) {
            if (!decodepath(argv[++i],types+n+1,paths[n+1],fmts+n+1)) return -1;
            n++;
        }
        else if (!strcmp(argv[i],"-p")&&i+3<argc) {
            pos[0]=atof(argv[++i])*D2R;
            pos[1]=atof(argv[++i])*D2R;
            pos[2]=atof(argv[++i]);
            pos2ecef(pos,stapos);
            stapos_set=1;
        }
        else if (!strcmp(argv[i],"-px")&&i+3<argc) {
            stapos[0]=atof(argv[++i]);
            stapos[1]=atof(argv[++i]);
            stapos[2]=atof(argv[++i]);
            stapos_set=1;
        }
        else if (!strcmp(argv[i],"-o")&&i+3<argc) {
            stadel[0]=atof(argv[++i]);
            stadel[1]=atof(argv[++i]);
            stadel[2]=atof(argv[++i]);
        }
        else if (!strcmp(argv[i],"-msg")&&i+1<argc) msg=argv[++i];
        else if (!strcmp(argv[i],"-opt")&&i+1<argc) opt=argv[++i];
        else if (!strcmp(argv[i],"-sta")&&i+1<argc) sta=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-d"  )&&i+1<argc) dispint=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-s"  )&&i+1<argc) opts[0]=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-r"  )&&i+1<argc) opts[1]=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-n"  )&&i+1<argc) opts[5]=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-f"  )&&i+1<argc) opts[6]=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-c"  )&&i+1<argc) cmdfile[0]=argv[++i];
        else if (!strcmp(argv[i],"-c1" )&&i+1<argc) cmdfile[1]=argv[++i];
        else if (!strcmp(argv[i],"-c2" )&&i+1<argc) cmdfile[2]=argv[++i];
        else if (!strcmp(argv[i],"-c3" )&&i+1<argc) cmdfile[3]=argv[++i];
        else if (!strcmp(argv[i],"-c4" )&&i+1<argc) cmdfile[4]=argv[++i];
        else if (!strcmp(argv[i],"-a"  )&&i+1<argc) antinfo=argv[++i];
        else if (!strcmp(argv[i],"-i"  )&&i+1<argc) rcvinfo=argv[++i];
        else if (!strcmp(argv[i],"-l"  )&&i+1<argc) local=argv[++i];
        else if (!strcmp(argv[i],"-x"  )&&i+1<argc) proxy=argv[++i];
        else if (!strcmp(argv[i],"-b"  )&&i+1<argc) opts[7]=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-fl" )&&i+1<argc) logfile=argv[++i];
        else if (!strcmp(argv[i],"-t"  )&&i+1<argc) trlevel=atoi(argv[++i]);
        else if (!strcmp(argv[i],"-pg" )) pgflag=1;
        else if (!strcmp(argv[i],"-nmea")&&i+1<argc) {
            char *spec=argv[++i],*colon=strchr(spec,':');
            if (colon) {
                int hlen=(int)(colon-spec);
                if (hlen>0&&hlen<(int)sizeof(nmea_svr_host)) {
                    memcpy(nmea_svr_host,spec,hlen);
                    nmea_svr_host[hlen]='\0';
                }
                nmea_svr_port=atoi(colon+1);
            } else {
                nmea_svr_port=atoi(spec);
            }
        }
        else if (*argv[i]=='-') printhelp();
    }
    if (n<=0) n=1; /* stdout */

    for (i=0;i<n;i++) {
        if (fmts[i+1]<=0) continue;
        if (fmts[i+1]!=STRFMT_RTCM3) {
            fprintf(stderr,"unsupported output format\n");
            return -1;
        }
        if (fmts[0]<0) {
            fprintf(stderr,"specify input format\n");
            return -1;
        }
        if (!(conv[i]=strconvnew(fmts[0],fmts[i+1],msg,sta,sta!=0,opt))) {
            fprintf(stderr,"stream conversion error\n");
            return -1;
        }
        strcpy(buff,antinfo);
        for (p=strtok(buff,","),j=0;p&&j<3;p=strtok(NULL,",")) ant[j++]=p;
        strcpy(conv[i]->out.sta.antdes,ant[0]);
        strcpy(conv[i]->out.sta.antsno,ant[1]);
        conv[i]->out.sta.antsetup=atoi(ant[2]);
        strcpy(buff,rcvinfo);
        for (p=strtok(buff,","),j=0;p&&j<3;p=strtok(NULL,",")) rcv[j++]=p;
        strcpy(conv[i]->out.sta.rectype,rcv[0]);
        strcpy(conv[i]->out.sta.recver ,rcv[1]);
        strcpy(conv[i]->out.sta.recsno ,rcv[2]);
        matcpy(conv[i]->out.sta.pos,stapos,3,1);
        matcpy(conv[i]->out.sta.del,stadel,3,1);
    }
    signal(SIGTERM,sigfunc);
    signal(SIGINT ,sigfunc);
    signal(SIGHUP ,SIG_IGN);
    signal(SIGPIPE,SIG_IGN);

    strsvrinit(&strsvr,n+1);

    /* setup pipe and serial device name for -pg mode */
    if (pgflag) {
        /* extract serial device name from output path for log message */
        for (i=1;i<=n;i++) {
            if (types[i]==STR_SERIAL) {
                strncpy(serialdev,paths[i],sizeof(serialdev)-1);
                break;
            }
        }
        /* test if serial is accessible */
        {
            char devpath[256];
            char *colon;
            sprintf(devpath,"/dev/%.*s",(int)(sizeof(devpath)-6),serialdev);
            if ((colon=strchr(devpath,':'))) *colon='\0';
            if (access(devpath,F_OK)!=0) {
                fprintf(stderr,"[GPS] cannot open serial://%s\n",serialdev);
                if (stapos_set) {
                    fprintf(stderr,"[GPS] serial unavailable, using -p position as fallback\n");
                    lock(&strsvr.nmeapos_lock);
                    matcpy(strsvr.nmeapos,stapos,3,1);
                    strsvr.nmeapos_valid=1;
                    unlock(&strsvr.nmeapos_lock);
                    gps_ready=1;
                } else {
                    fprintf(stderr,"failed to get initial position\n");
                }
                pgflag=0; /* disable pipe — serial not available */
            }
        }
    }

    /* if -p given without -pg, position is immediately valid */
    if (stapos_set&&!pgflag) strsvr.nmeapos_valid=1;

    if (pgflag) {
        fprintf(stderr,"[GPS] pipe\n");
        if (pipe(nmea_pipe)<0) {
            fprintf(stderr,"pipe error\n");
            return -1;
        }
        fcntl(nmea_pipe[1],F_SETFL,O_NONBLOCK);
        strsvr.serial_pipe_fd=nmea_pipe[1];
    }

    if (trlevel>0) {
        traceopen(*logfile?logfile:TRFILE);
        tracelevel(trlevel);
    }
    fprintf(stderr,"stream server start\n");

    strsetdir(local);
    strsetproxy(proxy);

    for (i=0;i<MAXSTR;i++) {
        if (*cmdfile[i]) readcmd(cmdfile[i],cmds[i],0);
        if (*cmdfile[i]) readcmd(cmdfile[i],cmds_periodic[i],2);
    }
    /* start stream server */
    if (!strsvrstart(&strsvr,opts,types,paths,logs,conv,cmds,cmds_periodic,
                     stapos)) {
        fprintf(stderr,"stream server start error\n");
        return -1;
    }

    /* start NMEA TCP server */
    if (nmea_svr_port>0) {
        nmeatcp=nmea_server_start(nmea_svr_host,nmea_svr_port,NULL);
        if (nmeatcp<0) {
            fprintf(stderr,"[NMEA] invalid port: %s\n",nmea_svr_host);
        } else {
            if (pthread_create(&nmea_tid,NULL,nmea_accept_thread,&nmeatcp)) {
                fprintf(stderr,"[NMEA] accept thread error\n");
            } else {
                pthread_detach(nmea_tid);
            }
        }
    }

    /* start GPS position thread and wait for first fix */
    if (pgflag) {
        if (pthread_create(&gps_tid,NULL,gps_pos_thread,&nmea_pipe[0])) {
            fprintf(stderr,"[GPS] thread error\n");
        } else {
            pthread_detach(gps_tid);
        }
        if (!gps_ready) {
            wait_initial_position(serialdev);
        }
    }

    for (intrflg=0;!intrflg;) {

        /* get stream server status */
        strsvrstat(&strsvr,stat,log_stat,byte,bps,strmsg);

        /* show stream server status */
        for (i=0,p=buff;i<MAXSTR;i++) p+=sprintf(p,"%c",ss[stat[i]+1]);

        fprintf(stderr,"%s [%s] %10d B %7d bps %s\n",
                time_str(utc2gpst(timeget()),0),buff,byte[0],bps[0],strmsg);

        sleepms(dispint);
    }
    for (i=0;i<MAXSTR;i++) {
        if (*cmdfile[i]) readcmd(cmdfile[i],cmds[i],1);
    }
    /* stop stream server */
    strsvrstop(&strsvr,cmds);

    /* close NMEA server */
    if (nmeatcp>=0) close(nmeatcp);
    pthread_mutex_lock(&nmea_mutex);
    for (i=0;i<nmea_nclient;i++) close(nmea_clients[i]);
    nmea_nclient=0;
    pthread_mutex_unlock(&nmea_mutex);

    /* close pipe */
    if (nmea_pipe[1]>=0) {
        strsvr.serial_pipe_fd=-1;
        close(nmea_pipe[1]);
    }
    if (nmea_pipe[0]>=0) close(nmea_pipe[0]);

    for (i=0;i<n;i++) {
        strconvfree(conv[i]);
    }
    if (trlevel>0) {
        traceclose();
    }
    fprintf(stderr,"stream server stop\n");
    return 0;
}
