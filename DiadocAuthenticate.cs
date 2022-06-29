using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Diadoc.Api;
using Diadoc.Api.Cryptography;
using Diadoc.Api.Proto.Events;

namespace AutoOrdersIntake
{		
	internal static class DiadocAuthenticate
	{
		//Аутентификация в диадоке
		public static string AuthenticateDiadoc()
        {
			Program.WriteLine("Аутентификация в диадоке");
			Program.WriteLine("=====================================");

			// Для использования API Диадока требуются:
			// 1. Крипто-API, предоставляемое операционной системой. Для систем на ОС Windows используйте класс WinApiCrypt.
			// 2. Экземпляр класса DiadocApi, проксирующий работу с Диадоком.

			var crypt = new WinApiCrypt();
			var diadocApi = new DiadocApi(
					"molkom-2332d712-912c-4c83-9eb5-e0041a40ef13",
					"https://diadoc-api.kontur.ru",
					crypt);

			// Большинству команд интеграторского интерфейса требуется авторизация.
			// Для этого команды требуют в качестве обязательного параметра так называемый авторизационный токен — массив байтов, однозначно идентифицирующий пользователя.
			// Один из способов авторизации — через логин и пароль пользователя:
			string authTokenByLogin = "";

			try
			{
				authTokenByLogin = diadocApi.Authenticate("tatarchuk_em@rossmol.ru", "dD080422@");
				Program.WriteLine("Успешная аутентификация по логину и паролю. Токен: " + authTokenByLogin);
				
			}
			catch (Exception e)
			{
				Program.WriteLine("Ошибка при аутентификации по логину и паролю.");
				Program.WriteLine(e.Message);
			}
			return authTokenByLogin;			

			// В дальнейшем полученный токен следует подставлять в те методы API, где он требуется. (PostMessage и т.п.)
			// Токен длится 24 часа, после его протухания методы начнут возвращать 401, и потребуется вновь получить токен через методы выше.
		}

		//Получение ID ЭДО провайдера для организации по ИНН
		public static string[] OrganizationInfo(string InnGpl, string KppGpl, string IdProvaider)
		{
			var crypt = new WinApiCrypt();
			var diadocApi = new DiadocApi(
					"molkom-2332d712-912c-4c83-9eb5-e0041a40ef13",
					"https://diadoc-api.kontur.ru",
					crypt);

			Program.WriteLine("Получение ID ЭДО провайдера");

			string FnsParticipantId = "";
			string BoxId = "";
			var OrganizationList = diadocApi.GetOrganizationsByInnKpp(InnGpl, KppGpl);
			foreach (var organization in OrganizationList.Organizations)
            {
				if(organization.FnsParticipantId.Substring(0,3)== IdProvaider)  //Ищем организации, соответствующие провайдеру плательщика
				{
					Program.WriteLine("FnsParticipantId " + organization.FnsParticipantId);
					Program.WriteLine("FullName " + organization.FullName);
					Program.WriteLine("Количество Box " + organization.Boxes.Count);
					for (int j = 0; j < organization.Boxes.Count; j++)
                    {
						Program.WriteLine("BoxId " + organization.Boxes[j].BoxId);
						Program.WriteLine("Title " + organization.Boxes[j].Title);
						BoxId = organization.Boxes[j].BoxId;
					}
					
					FnsParticipantId = organization.FnsParticipantId;
				}
			}
			string[] result = new string[2];
			result[0] = FnsParticipantId;
			result[1] = BoxId;
			return result;	

		}

		//Получение данных организации по ID ЭДО организации
		public static string[] OrganizationInfo_forfnsParticipantId(string fnsParticipantId)
		{
			var crypt = new WinApiCrypt();
			var diadocApi = new DiadocApi(
					"molkom-2332d712-912c-4c83-9eb5-e0041a40ef13",
					"https://diadoc-api.kontur.ru",
					crypt);

			Program.WriteLine("Получение данных организации по ID ЭДО организации");
			
			string BoxId = "";
			string Inn = "";
			string Kpp = "";

			var Organization = diadocApi.GetOrganizationByFnsParticipantId(fnsParticipantId);				
			
			Program.WriteLine("FnsParticipantId " + Organization.FnsParticipantId);
			Program.WriteLine("FullName " + Organization.FullName);
			Program.WriteLine("Количество Box " + Organization.Boxes.Count);
			for (int j = 0; j < Organization.Boxes.Count; j++)
			{
				Program.WriteLine("BoxId " + Organization.Boxes[j].BoxId);
				Program.WriteLine("Title " + Organization.Boxes[j].Title);
				BoxId = Organization.Boxes[j].BoxId;
			}

			Inn = Organization.Inn;
			Kpp = Organization.Kpp;

			string[] result = new string[3];
			result[0] = BoxId;
			result[1] = Inn;
			result[2] = Kpp;
			return result;

		}


		//Отправка счета-фактуры
		public static void SendInvoiceXml(string pathUPDEDI, string fileName, string idPol, string idOtpr, string BoxIdPol, string BoxIdOtpr, string typeNamedId, string documentNumber, string documentDate)
		{
			Program.WriteLine("Начинаем отправку " + documentNumber);
			Program.WriteLine("Файл " + fileName);

			string NonformalizedDocumentPath = pathUPDEDI + fileName;
			
		    var crypt = new WinApiCrypt();

			var diadocApi = new DiadocApi(
					"molkom-2332d712-912c-4c83-9eb5-e0041a40ef13",
					"https://diadoc-api.kontur.ru",
					crypt);

			string authTokenByLogin = "";

			try
			{
				authTokenByLogin = diadocApi.Authenticate("tatarchuk_em@rossmol.ru", "dD080422@");
				Program.WriteLine("Успешная аутентификация по логину и паролю. Токен: " + authTokenByLogin);

			}
			catch (Exception e)
			{
				Program.WriteLine("Ошибка при аутентификации по логину и паролю.");
				Program.WriteLine(e.Message);
			}
			
			Program.WriteLine("Путь " + NonformalizedDocumentPath);			

			try
			{
				var content = File.ReadAllBytes(NonformalizedDocumentPath);				

				var documentAttachment = new DocumentAttachment
				{
					TypeNamedId = typeNamedId,

					SignedContent = new SignedContent
					{
						Content = content,
					}
				};

				Program.WriteLine("FileName " + fileName);
				Program.WriteLine("SellerFnsParticipantId " + idOtpr);
				Program.WriteLine("BuyerFnsParticipantId " + idPol);
				Program.WriteLine("DocumentDate " + documentDate);
				Program.WriteLine("DocumentNumber " + documentNumber);				

				var messageToPost = new MessageToPost
				{
					FromBoxId = BoxIdOtpr,
					ToBoxId = BoxIdPol,
					IsDraft = true //флаг, показывающий, что данное сообщение является черновиком				
				};				

				// Добавим информацию о документе в MessageToPost:
				messageToPost.DocumentAttachments.Add(documentAttachment);				

				var response = diadocApi.PostMessage(authTokenByLogin, messageToPost);				

				// При необходимости можно обработать ответ сервера (например, можно получить
				// и сохранить для последующей обработки идентификатор сообщения)
				Program.WriteLine("Документ был успешно загружен.");
				Program.WriteLine("MessageID: " + response.MessageId);
				//return response;

			}
			catch (Exception e)
			{
				Program.WriteLine("Ошибка чтения и отправки файла");
				Program.WriteLine(e.Message);				
			}
			
		}
	}
}
