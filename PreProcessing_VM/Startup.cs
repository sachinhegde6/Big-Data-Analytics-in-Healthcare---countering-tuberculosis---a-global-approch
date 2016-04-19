using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(PreProcessing_VM.Startup))]
namespace PreProcessing_VM
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
