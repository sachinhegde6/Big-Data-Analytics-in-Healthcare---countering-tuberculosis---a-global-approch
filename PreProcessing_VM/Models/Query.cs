using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace PreProcessing_VM.Models
{
    public class Query
    {
        [Required]
        [EmailAddress]
        [Display(Name = "Hive Query")]
        public string HiveQuery{ get; set; }
    }
}